package config

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb"
)

// Config store the crendentials of influxdb host and token.
type Config struct {
	Name string `toml:"-" json:"-"`
	Host string `toml:"url" json:"url"`
	// Token is base64 encoded sequence.
	Token          string `toml:"token" json:"token"`
	Org            string `toml:"org" json:"org"`
	Active         bool   `toml:"active,omitempty" json:"active,omitempty"`
	PreviousActive bool   `toml:"previous,omitempty" json:"previous,omitempty"`
}

// DefaultConfig is default config without token
var DefaultConfig = Config{
	Name:   "default",
	Host:   "http://localhost:9999",
	Active: true,
}

// Configs is map of configs indexed by name.
type Configs map[string]Config

// ConfigsService is the service to list and write configs.
type ConfigsService interface {
	WriteConfigs(pp Configs) error
	ParseConfigs() (Configs, error)
	ParsePreviousActiveConfig() (Config, error)
}

// Switch to another config.
func (pp *Configs) Switch(name string) error {
	pc := *pp
	if _, ok := pc[name]; !ok {
		return &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf(`config %q is not found`, name),
		}
	}
	for k, v := range pc {
		v.PreviousActive = v.Active
		v.Active = k == name
		pc[k] = v
	}
	return nil
}

// LocalConfigsSVC has the path and dir to write and parse configs.
type LocalConfigsSVC struct {
	Path string
	Dir  string
}

// ParseConfigs from the local path.
func (svc LocalConfigsSVC) ParseConfigs() (Configs, error) {
	r, err := os.Open(svc.Path)
	if err != nil {
		return make(Configs), nil
	}
	return ParseConfigs(r)
}

// ParsePreviousActiveConfig from the local path.
func (svc LocalConfigsSVC) ParsePreviousActiveConfig() (Config, error) {
	r, err := os.Open(svc.Path)
	if err != nil {
		return Config{}, nil
	}
	return ParsePreviousActive(r)
}

func blockBadName(pp Configs) error {
	for n := range pp {
		if n == "-" {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `"-" is not a valid config name`,
			}
		}
	}
	return nil
}

func writeConfigs(pp Configs, w io.Writer) error {
	if err := blockBadName(pp); err != nil {
		return err
	}
	var b2 bytes.Buffer
	if err := toml.NewEncoder(w).Encode(pp); err != nil {
		return err
	}
	// a list cloud 2 clusters, commented out
	w.Write([]byte("# \n"))
	pp = map[string]Config{
		"us-central": {Host: "https://us-central1-1.gcp.cloud2.influxdata.com", Token: "XXX"},
		"us-west":    {Host: "https://us-west-2-1.aws.cloud2.influxdata.com", Token: "XXX"},
		"eu-central": {Host: "https://eu-central-1-1.aws.cloud2.influxdata.com", Token: "XXX"},
	}

	if err := toml.NewEncoder(&b2).Encode(pp); err != nil {
		return err
	}
	reader := bufio.NewReader(&b2)
	for {
		line, _, err := reader.ReadLine()

		if err == io.EOF {
			break
		}
		w.Write([]byte("# " + string(line) + "\n"))
	}
	return nil
}

// WriteConfigs to the path.
func (svc LocalConfigsSVC) WriteConfigs(pp Configs) error {
	if err := os.MkdirAll(svc.Dir, os.ModePerm); err != nil {
		return err
	}
	var b1 bytes.Buffer
	if err := writeConfigs(pp, &b1); err != nil {
		return err
	}
	return ioutil.WriteFile(svc.Path, b1.Bytes(), 0600)
}

// ParseConfigs decodes configs from io readers
func ParseConfigs(r io.Reader) (Configs, error) {
	pp := make(Configs)
	_, err := toml.DecodeReader(r, &pp)
	for n, p := range pp {
		p.Name = n
		pp[n] = p
	}
	return pp, err
}

// ParsePreviousActive return the previous active config from the reader
func ParsePreviousActive(r io.Reader) (Config, error) {
	return parseActiveConfig(r, false)
}

// ParseActiveConfig returns the active config from the reader.
func ParseActiveConfig(r io.Reader) (Config, error) {
	return parseActiveConfig(r, true)
}

func parseActiveConfig(r io.Reader, currentOrPrevious bool) (Config, error) {
	previousText := ""
	if !currentOrPrevious {
		previousText = "previous "
	}
	pp, err := ParseConfigs(r)
	if err != nil {
		return DefaultConfig, err
	}
	var activated Config
	var hasActive bool
	for _, p := range pp {
		check := p.Active
		if !currentOrPrevious {
			check = p.PreviousActive
		}
		if check && !hasActive {
			activated = p
			hasActive = true
		} else if check {
			return DefaultConfig, &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  "more than one " + previousText + "activated configs found",
			}
		}
	}
	if hasActive {
		return activated, nil
	}
	return DefaultConfig, &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  previousText + "activated config is not found",
	}
}
