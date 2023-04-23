module github.com/datawire/go-fuseftp

go 1.20

require (
	github.com/datawire/dlib v1.3.1-0.20220715022530-b09ab2e017e1
	github.com/datawire/go-ftpserver v0.1.3
	github.com/datawire/go-fuseftp/rpc v0.3.0
	github.com/jlaffaye/ftp v0.1.0
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.1
	github.com/winfsp/cgofuse v1.5.0
	golang.org/x/sys v0.7.0
	google.golang.org/grpc v1.54.0
	google.golang.org/protobuf v1.30.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fclairamb/ftpserverlib v0.21.0 // indirect
	github.com/fclairamb/go-log v0.4.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/afero v1.9.5 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/datawire/go-fuseftp/rpc => ./rpc
