package mainio

type Config struct {
	Access   string
	Secret   string
	Bucket   string
	Endpoint string
}

type Client struct {
}

func New(config *Config) (*Client, error) {
	return nil, nil
}
