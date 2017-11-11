package bucketsync

type Repository interface {
	SaveDirectory(target Directory) error
	SaveFile(target File) error
	SaveSymLink(target SymLink) error
	GetDirectory(key ObjectKey) (Directory, error)
	GetFile(key ObjectKey) (File, error)
	GetSymLink(key ObjectKey) (SymLink, error)
	GetMeta(key ObjectKey) (Meta, error)
}
