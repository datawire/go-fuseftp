module github.com/datawire/go-fuseftp

go 1.18

require (
	github.com/datawire/dlib v1.3.1-0.20220715022530-b09ab2e017e1
	github.com/fclairamb/ftpserverlib v0.19.0
	github.com/fclairamb/go-log v0.4.1
	github.com/jlaffaye/ftp v0.0.0-20220818164422-4d1d644cf19d
	github.com/spf13/afero v1.9.2
	github.com/stretchr/testify v1.8.0
	github.com/winfsp/cgofuse v1.5.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	golang.org/x/sys v0.0.0-20220818161305-2296e01440c6 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// The Get/MLST command is currently in PR https://github.com/jlaffaye/ftp/pull/269
replace github.com/jlaffaye/ftp v0.0.0-20220818164422-4d1d644cf19d => github.com/thallgren/ftp v0.0.0-20220819061632-13fe07fc1b57
