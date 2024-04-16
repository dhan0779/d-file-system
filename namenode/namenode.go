package namenode

type NameNodeService struct {
	BlockSize uint64
	ReplicationFactor uint64
	Port uint64
}

func New(blockSize uint64, replicationFactor uint64) *NameNodeService {
	return &NameNodeService{
		BlockSize: blockSize,
		ReplicationFactor: replicationFactor,
	}
}