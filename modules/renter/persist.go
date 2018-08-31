package renter

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/encoding"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/renter/siafile"
	"gitlab.com/NebulousLabs/Sia/persist"
	"gitlab.com/NebulousLabs/Sia/types"
)

const (
	logFile = modules.RenterDir + ".log"
	// PersistFilename is the filename to be used when persisting renter information to a JSON file
	PersistFilename = "renter.json"
	// ShareExtension is the extension to be used
	ShareExtension = ".sia"
	// SiaDirMetadata is the name of the metadata file for the sia directory
	SiaDirMetadata = ".siadir"
)

var (
	//ErrBadFile is an error when a file does not qualify as .sia file
	ErrBadFile = errors.New("not a .sia file")
	// ErrIncompatible is an error when file is not compatible with current version
	ErrIncompatible = errors.New("file is not compatible with current version")
	// ErrNoNicknames is an error when no nickname is given
	ErrNoNicknames = errors.New("at least one nickname must be supplied")
	// ErrNonShareSuffix is an error when the suffix of a file does not match the defined share extension
	ErrNonShareSuffix = errors.New("suffix of file must be " + ShareExtension)

	dirMetadataHeader = persist.Metadata{
		Header:  "Sia Directory Metadata",
		Version: persistVersion,
	}
	settingsMetadata = persist.Metadata{
		Header:  "Renter Persistence",
		Version: persistVersion,
	}

	shareHeader  = [15]byte{'S', 'i', 'a', ' ', 'S', 'h', 'a', 'r', 'e', 'd', ' ', 'F', 'i', 'l', 'e'}
	shareVersion = "0.4"

	// Persist Version Numbers
	persistVersion040 = "0.4"
	persistVersion133 = "1.3.3"

	// timeBetweenRepair is the amount of time to wait before trying to repair a
	// file again.  This is to prevent one bad file being repaired continuosly
	// while other files degrade
	//
	// TODO - threadedUpload loop only runs every 15mins, unless renter uploads
	// file. How should this impact the value of timeBetweenRepair
	timeBetweenRepair = func() int64 {
		switch build.Release {
		case "dev":
			return int64(time.Minute * 5)
		case "standard":
			return int64(time.Hour * 2)
		case "testing":
			return int64(time.Second * 5)
		}
		panic("undefined timeBetweenRepair")
	}()
)

type (
	// dirMetadata contains the metadata information about a renter directory
	dirMetadata struct {
		LastRepair    int64
		LastUpdate    int64
		MinRedundancy float64
		NumSiaFiles   int
	}

	// persist contains all of the persistent renter data.
	persistence struct {
		MaxDownloadSpeed int64
		MaxUploadSpeed   int64
		StreamCacheSize  uint64
		Tracking         map[string]trackedFile
	}
)

// MarshalSia implements the encoding.SiaMarshaller interface, writing the
// file data to w.
func (f *file) MarshalSia(w io.Writer) error {
	enc := encoding.NewEncoder(w)

	// encode easy fields
	err := enc.EncodeAll(
		f.name,
		f.size,
		f.masterKey,
		f.pieceSize,
		f.mode,
	)
	if err != nil {
		return err
	}
	// COMPATv0.4.3 - encode the bytesUploaded and chunksUploaded fields
	// TODO: the resulting .sia file may confuse old clients.
	err = enc.EncodeAll(f.pieceSize*f.numChunks()*uint64(f.erasureCode.NumPieces()), f.numChunks())
	if err != nil {
		return err
	}

	// encode erasureCode
	switch code := f.erasureCode.(type) {
	case *rsCode:
		err = enc.EncodeAll(
			"Reed-Solomon",
			uint64(code.dataPieces),
			uint64(code.numPieces-code.dataPieces),
		)
		if err != nil {
			return err
		}
	default:
		if build.DEBUG {
			panic("unknown erasure code")
		}
		return errors.New("unknown erasure code")
	}
	// encode contracts
	if err := enc.Encode(uint64(len(f.contracts))); err != nil {
		return err
	}
	for _, c := range f.contracts {
		if err := enc.Encode(c); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalSia implements the encoding.SiaUnmarshaller interface,
// reconstructing a file from the encoded bytes read from r.
func (f *file) UnmarshalSia(r io.Reader) error {
	dec := encoding.NewDecoder(r)

	// COMPATv0.4.3 - decode bytesUploaded and chunksUploaded into dummy vars.
	var bytesUploaded, chunksUploaded uint64

	// Decode easy fields.
	err := dec.DecodeAll(
		&f.name,
		&f.size,
		&f.masterKey,
		&f.pieceSize,
		&f.mode,
		&bytesUploaded,
		&chunksUploaded,
	)
	if err != nil {
		return err
	}
	f.staticUID = persist.RandomSuffix()

	// Decode erasure coder.
	var codeType string
	if err := dec.Decode(&codeType); err != nil {
		return err
	}
	switch codeType {
	case "Reed-Solomon":
		var nData, nParity uint64
		err = dec.DecodeAll(
			&nData,
			&nParity,
		)
		if err != nil {
			return err
		}
		rsc, err := NewRSCode(int(nData), int(nParity))
		if err != nil {
			return err
		}
		f.erasureCode = rsc
	default:
		return errors.New("unrecognized erasure code type: " + codeType)
	}

	// Decode contracts.
	var nContracts uint64
	if err := dec.Decode(&nContracts); err != nil {
		return err
	}
	f.contracts = make(map[types.FileContractID]fileContract)
	var contract fileContract
	for i := uint64(0); i < nContracts; i++ {
		if err := dec.Decode(&contract); err != nil {
			return err
		}
		f.contracts[contract.ID] = contract
	}
	return nil
}

// createDir creates directory in the renter directory
func (r *Renter) createDir(siapath string) error {
	// Enforce nickname rules.
	if err := validateSiapath(siapath); err != nil {
		return err
	}

	// Create direcotry
	path := filepath.Join(r.persistDir, siapath)
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}

	// Make sure all parent directories have metadata files
	//
	// TODO: this should be change when files are moved out of the top level
	// directory of the renter.
	for path != filepath.Dir(r.persistDir) {
		if err := r.createDirMetadata(path); err != nil {
			return err
		}
		path = filepath.Dir(path)
	}
	return nil
}

// createDirMetadata makes sure there is a metadata file in the directory
// creates one as needed
func (r *Renter) createDirMetadata(path string) error {
	fullPath := filepath.Join(path, SiaDirMetadata)
	// Check if metadata file exists
	if _, err := os.Stat(fullPath); err == nil {
		return nil
	}

	// Initialize metadata
	data := dirMetadata{
		LastRepair:    int64(0),
		LastUpdate:    time.Now().UnixNano(),
		MinRedundancy: float64(0),
		NumSiaFiles:   0,
	}
	return r.saveDirMetadata(path, data)
}

// findMinDirRedundancy walks the renter's persistance directory and finds the
// directory with the lowest redundancy. Since it uses Walk() the directory
// returned will be the lowest level directory
//
// TODO - This could be quicker if we just followed the path of lowest
// redundancy instead of walking the directory. Will need to make sure that
// managedUpdateRenterRedundancy makes sure that each directory's redundancy is
// the lowest redundancy of any of its files or sub directories
func (r *Renter) findMinDirRedundancy() (string, error) {
	var metadata, metadataAnyTime dirMetadata
	dir := r.persistDir
	redundancy := math.MaxFloat64
	// dirAnyTime and redundancyAnyTime is the dir and redundancy regardless of
	// timeBetweenRepair
	dirAnyTime := r.persistDir
	redundancyAnyTime := math.MaxFloat64

	// Read renter persist directory metadata
	persistDirMetadata, err := r.loadDirMetadata(dir)
	if err != nil {
		r.log.Printf("WARN: Could not load directory metadata for %v: %v", dir, err)
		return dir, err
	}
	_ = filepath.Walk(r.persistDir, func(path string, info os.FileInfo, err error) error {
		// This Walk will log errors but not return them

		// Skip files
		//
		// TODO: Currently skipping contracts directory until renter files are
		// not stored in the top level directory of the renter.  Then the
		// starting point will be the renter's files directory and not
		// r.persistDir
		contractsDir := filepath.Join(r.persistDir, "contracts")
		if !info.IsDir() || path == contractsDir {
			return nil
		}

		// Read directory metadata
		md, err := r.loadDirMetadata(path)
		if err != nil {
			r.log.Printf("WARN: Could not load directory metadata for %v: %v", path, err)
			return nil
		}

		// Skip empty directories
		if md.NumSiaFiles == 0 {
			return nil
		}

		// Check redundancy
		if md.MinRedundancy > redundancy {
			return nil
		}
		metadataAnyTime = md
		redundancyAnyTime = md.MinRedundancy
		dirAnyTime = path
		if md.LastRepair < time.Now().UnixNano()-timeBetweenRepair {
			metadata = md
			redundancy = md.MinRedundancy
			dir = path
		}
		return nil
	})

	if dir == r.persistDir && persistDirMetadata.LastRepair >= time.Now().UnixNano()-timeBetweenRepair {
		metadataAnyTime.LastRepair = time.Now().UnixNano()
		return dirAnyTime, r.saveDirMetadata(dirAnyTime, metadataAnyTime)
	}
	metadata.LastRepair = time.Now().UnixNano()
	return dir, r.saveDirMetadata(dir, metadata)
}

// loadDirMetadata loads the directory metadata from disk
func (r *Renter) loadDirMetadata(path string) (dirMetadata, error) {
	var metadata dirMetadata
	err := persist.LoadJSON(dirMetadataHeader, &metadata, filepath.Join(path, SiaDirMetadata))
	if os.IsNotExist(err) {
		if err = r.createDirMetadata(path); err != nil {
			return metadata, err
		}
		return r.loadDirMetadata(path)
	}
	if err != nil {
		return metadata, err
	}
	return metadata, nil
}

// saveDirMetadata saves the directory metadata to disk
func (r *Renter) saveDirMetadata(path string, metadata dirMetadata) error {
	return persist.SaveJSON(dirMetadataHeader, metadata, filepath.Join(path, SiaDirMetadata))
}

// updateDirMetadata updates all the renter's directories' metadata
func (r *Renter) updateDirMetadata(redundancies map[string]float64) error {
	for path, redundancy := range redundancies {
		// Update number of files
		files, err := ioutil.ReadDir(path)
		if err != nil {
			return err
		}
		siaFiles := 0
		for _, f := range files {
			if filepath.Ext(f.Name()) == ShareExtension {
				siaFiles++
				continue
			}
		}
		metadata, err := r.loadDirMetadata(path)
		if err != nil {
			return err
		}
		err = r.saveDirMetadata(path, dirMetadata{
			LastRepair:    metadata.LastRepair,
			LastUpdate:    time.Now().UnixNano(),
			MinRedundancy: redundancy,
			NumSiaFiles:   siaFiles,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// saveFile saves a file to the renter directory.
func (r *Renter) saveFile(f *siafile.SiaFile) error {
	if f.Deleted() { // TODO: violation of locking convention
		return errors.New("can't save deleted file")
	}
	// Create directory structure specified in nickname, only for files not in
	// top level renter directory
	dir := filepath.Dir(f.SiaPath())
	if dir != "." {
		err := r.createDir(dir)
		if err != nil {
			return err
		}
	}

	// Open SafeFile handle.
	fullPath := filepath.Join(r.persistDir, f.SiaPath()+ShareExtension)
	handle, err := persist.NewSafeFile(fullPath)
	if err != nil {
		return err
	}
	defer handle.Close()

	// Write file data.
	err = r.shareFiles([]*siafile.SiaFile{f}, handle)
	if err != nil {
		return err
	}

	// Commit the SafeFile.
	return handle.CommitSync()
}

// saveSync stores the current renter data to disk and then syncs to disk.
func (r *Renter) saveSync() error {
	return persist.SaveJSON(settingsMetadata, r.persist, filepath.Join(r.persistDir, PersistFilename))
}

// loadSiaFiles walks through the directory searching for siafiles and loading
// them into memory.
func (r *Renter) loadSiaFiles() error {
	// Recursively load all files found in renter directory. Errors
	// encountered during loading are logged, but are not considered fatal.
	return filepath.Walk(r.persistDir, func(path string, info os.FileInfo, err error) error {
		// This error is non-nil if filepath.Walk couldn't stat a file or
		// folder.
		if err != nil {
			r.log.Println("WARN: could not stat file or folder during walk:", err)
			return nil
		}

		// Skip folders and non-sia files.
		if info.IsDir() || filepath.Ext(path) != ShareExtension {
			return nil
		}

		// Open the file.
		file, err := os.Open(path)
		if err != nil {
			r.log.Println("ERROR: could not open .sia file:", err)
			return nil
		}
		defer file.Close()

		// Load the file contents into the renter.
		_, err = r.loadSharedFiles(file)
		if err != nil {
			r.log.Println("ERROR: could not load .sia file:", err)
			return nil
		}
		return nil
	})
}

// readDirSiaFiles reads the sia files in the directory and returns them as a
// map of sia files
func (r *Renter) readDirSiaFiles(path string) map[string]*siafile.SiaFile {
	// This method will log errors and continue to try and return as many files
	// as possible

	// Make map for sia files
	siaFiles := make(map[string]*siafile.SiaFile)

	// Read directory
	finfos, err := ioutil.ReadDir(path)
	if err != nil {
		r.log.Println("WARN: Error in reading files in least redundant directory:", err)
		return siaFiles
	}

	for _, fi := range finfos {
		filename := filepath.Join(path, fi.Name())
		// Open the file.
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			r.log.Println("ERROR: could not open .sia file:", err)

		}

		// Read the file contents and add to map.
		files, err := r.readSharedFiles(file)
		if err != nil {
			r.log.Println("ERROR: could not read .sia file:", err)
			continue
		}
		for k, v := range files {
			siaFiles[k] = v
		}

	}
	return siaFiles
}

// load fetches the saved renter data from disk.
func (r *Renter) loadSettings() error {
	r.persist = persistence{
		Tracking: make(map[string]trackedFile),
	}
	err := persist.LoadJSON(settingsMetadata, &r.persist, filepath.Join(r.persistDir, PersistFilename))
	if os.IsNotExist(err) {
		// No persistence yet, set the defaults and continue.
		r.persist.MaxDownloadSpeed = DefaultMaxDownloadSpeed
		r.persist.MaxUploadSpeed = DefaultMaxUploadSpeed
		r.persist.StreamCacheSize = DefaultStreamCacheSize
		err = r.saveSync()
		if err != nil {
			return err
		}
	} else if err == persist.ErrBadVersion {
		// Outdated version, try the 040 to 133 upgrade.
		err = convertPersistVersionFrom040To133(filepath.Join(r.persistDir, PersistFilename))
		if err != nil {
			// Nothing left to try.
			return err
		}
		// Re-load the settings now that the file has been upgraded.
		return r.loadSettings()
	} else if err != nil {
		return err
	}

	// Set the bandwidth limits on the contractor, which was already initialized
	// without bandwidth limits.
	return r.setBandwidthLimits(r.persist.MaxDownloadSpeed, r.persist.MaxUploadSpeed)
}

// shareFiles writes the specified files to w. First a header is written,
// followed by the gzipped concatenation of each file.
func (r *Renter) shareFiles(siaFiles []*siafile.SiaFile, w io.Writer) error {
	// Convert files to old type.
	files := make([]*file, 0, len(siaFiles))
	for _, sf := range siaFiles {
		files = append(files, r.siaFileToFile(sf))
	}
	// Write header.
	err := encoding.NewEncoder(w).EncodeAll(
		shareHeader,
		shareVersion,
		uint64(len(files)),
	)
	if err != nil {
		return err
	}

	// Create compressor.
	zip, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
	enc := encoding.NewEncoder(zip)

	// Encode each file.
	for _, f := range files {
		err = enc.Encode(f)
		if err != nil {
			return err
		}
	}

	return zip.Close()
}

// ShareFiles saves the specified files to shareDest.
func (r *Renter) ShareFiles(nicknames []string, shareDest string) error {
	lockID := r.mu.RLock()
	defer r.mu.RUnlock(lockID)

	// TODO: consider just appending the proper extension.
	if filepath.Ext(shareDest) != ShareExtension {
		return ErrNonShareSuffix
	}

	handle, err := os.Create(shareDest)
	if err != nil {
		return err
	}
	defer handle.Close()

	// Load files from renter.
	files := make([]*siafile.SiaFile, len(nicknames))
	for i, name := range nicknames {
		f, exists := r.files[name]
		if !exists {
			return ErrUnknownPath
		}
		files[i] = f
	}

	err = r.shareFiles(files, handle)
	if err != nil {
		os.Remove(shareDest)
		return err
	}

	return nil
}

// ShareFilesASCII returns the specified files in ASCII format.
func (r *Renter) ShareFilesASCII(nicknames []string) (string, error) {
	lockID := r.mu.RLock()
	defer r.mu.RUnlock(lockID)

	// Load files from renter.
	files := make([]*siafile.SiaFile, len(nicknames))
	for i, name := range nicknames {
		f, exists := r.files[name]
		if !exists {
			return "", ErrUnknownPath
		}
		files[i] = f
	}

	buf := new(bytes.Buffer)
	err := r.shareFiles(files, base64.NewEncoder(base64.URLEncoding, buf))
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

// loadSharedFiles reads .sia data from reader and registers the contained
// files in the renter. It returns the nicknames of the loaded files.
func (r *Renter) loadSharedFiles(reader io.Reader) ([]string, error) {
	// read header
	var header [15]byte
	var version string
	var numFiles uint64
	err := encoding.NewDecoder(reader).DecodeAll(
		&header,
		&version,
		&numFiles,
	)
	if err != nil {
		return nil, err
	} else if header != shareHeader {
		return nil, ErrBadFile
	} else if version != shareVersion {
		return nil, ErrIncompatible
	}

	// Create decompressor.
	unzip, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	dec := encoding.NewDecoder(unzip)

	// Read each file.
	files := make([]*file, numFiles)
	rlock := r.mu.RLock()
	for i := range files {
		files[i] = new(file)
		err := dec.Decode(files[i])
		if err != nil {
			return nil, err
		}

		// Make sure the file's name does not conflict with existing files.
		dupCount := 0
		origName := files[i].name
		for {
			_, exists := r.files[files[i].name]
			if !exists {
				break
			}
			dupCount++
			files[i].name = origName + "_" + strconv.Itoa(dupCount)
		}
	}
	r.mu.RUnlock(rlock)

	// Add files to renter.
	names := make([]string, numFiles)
	lock := r.mu.Lock()
	defer r.mu.Unlock(lock)
	for i, f := range files {
		r.files[f.name] = r.fileToSiaFile(f, r.persist.Tracking[f.name].RepairPath)
		names[i] = f.name
	}
	// Save the files.
	for _, f := range files {
		r.saveFile(r.fileToSiaFile(f, r.persist.Tracking[f.name].RepairPath))
	}

	return names, nil
}

// readSharedFiles reads .sia data from reader and returns a map of sia files
func (r *Renter) readSharedFiles(reader io.Reader) (map[string]*siafile.SiaFile, error) {
	// read header
	var header [15]byte
	var version string
	var numFiles uint64
	err := encoding.NewDecoder(reader).DecodeAll(
		&header,
		&version,
		&numFiles,
	)
	if err != nil {
		return nil, err
	} else if header != shareHeader {
		return nil, ErrBadFile
	} else if version != shareVersion {
		return nil, ErrIncompatible
	}

	// Create decompressor.
	unzip, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	dec := encoding.NewDecoder(unzip)

	// Read each file.
	files := make([]*file, numFiles)
	for i := range files {
		files[i] = new(file)
		err := dec.Decode(files[i])
		if err != nil {
			return nil, err
		}

		// Make sure the file's name does not conflict with existing files.
		dupCount := 0
		origName := files[i].name
		for {
			_, exists := r.files[files[i].name]
			if !exists {
				break
			}
			dupCount++
			files[i].name = origName + "_" + strconv.Itoa(dupCount)
		}
	}

	// Build map of sia files.
	siaFiles := make(map[string]*siafile.SiaFile)
	for _, f := range files {
		siaFiles[f.name] = r.fileToSiaFile(f, r.persist.Tracking[f.name].RepairPath)
	}

	return siaFiles, nil
}

// initPersist handles all of the persistence initialization, such as creating
// the persistence directory and starting the logger.
func (r *Renter) initPersist() error {
	// Create the persist directory if it does not yet exist.
	err := os.MkdirAll(r.persistDir, 0700)
	if err != nil {
		return err
	}

	// Initialize the logger.
	r.log, err = persist.NewFileLogger(filepath.Join(r.persistDir, logFile))
	if err != nil {
		return err
	}

	// Load the prior persistence structures.
	err = r.loadSettings()
	if err != nil {
		return err
	}

	// Load the siafiles into memory.
	return r.loadSiaFiles()
}

// LoadSharedFiles loads a .sia file into the renter. It returns the nicknames
// of the loaded files.
func (r *Renter) LoadSharedFiles(filename string) ([]string, error) {
	lockID := r.mu.Lock()
	defer r.mu.Unlock(lockID)

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return r.loadSharedFiles(file)
}

// LoadSharedFilesASCII loads an ASCII-encoded .sia file into the renter. It
// returns the nicknames of the loaded files.
func (r *Renter) LoadSharedFilesASCII(asciiSia string) ([]string, error) {
	lockID := r.mu.Lock()
	defer r.mu.Unlock(lockID)

	dec := base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(asciiSia))
	return r.loadSharedFiles(dec)
}

// convertPersistVersionFrom040to133 upgrades a legacy persist file to the next
// version, adding new fields with their default values.
func convertPersistVersionFrom040To133(path string) error {
	metadata := persist.Metadata{
		Header:  settingsMetadata.Header,
		Version: persistVersion040,
	}
	p := persistence{
		Tracking: make(map[string]trackedFile),
	}

	err := persist.LoadJSON(metadata, &p, path)
	if err != nil {
		return err
	}
	metadata.Version = persistVersion133
	p.MaxDownloadSpeed = DefaultMaxDownloadSpeed
	p.MaxUploadSpeed = DefaultMaxUploadSpeed
	p.StreamCacheSize = DefaultStreamCacheSize
	return persist.SaveJSON(metadata, p, path)
}
