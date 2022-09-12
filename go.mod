module github.com/datawire/go-fuseftp

go 1.18

require (
	github.com/datawire/dlib v1.3.1-0.20220715022530-b09ab2e017e1
	github.com/datawire/go-fuseftp/rpc v0.1.0
	github.com/fclairamb/ftpserverlib v0.19.1
	github.com/fclairamb/go-log v0.4.1
	github.com/jlaffaye/ftp v0.0.0-20220821212529-0aeb8660a7e2
	github.com/spf13/afero v1.9.2
	github.com/stretchr/testify v1.8.0
	github.com/winfsp/cgofuse v1.5.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	golang.org/x/net v0.0.0-20220517181318-183a9ca12b87 // indirect
	golang.org/x/sys v0.0.0-20220818161305-2296e01440c6 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220505152158-f39f71e6c8f3 // indirect
	google.golang.org/grpc v1.46.2 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/datawire/go-fuseftp/rpc => ./rpc
