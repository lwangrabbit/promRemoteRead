package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
)

var (
	// DefaultRemoteReadConfig is the default remote read configuration.
	DefaultRemoteReadConfig = RemoteReadConfig{
		RemoteTimeout: model.Duration(1 * time.Minute),
	}
)

func Load(s string) (*Config, error) {
	cfg := &Config{}
	err := yaml.Unmarshal([]byte(s), cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func LoadFile(filename string) (*Config, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	cfg, err := Load(string(content))
	if err != nil {
		return nil, errors.Wrapf(err, "parsing YAML file %s", filename)
	}
	resolveFilepaths(filepath.Dir(filename), cfg)
	return cfg, nil
}

func resolveFilepaths(baseDir string, cfg *Config) {
	join := func(fp string) string {
		if len(fp) > 0 && !filepath.IsAbs(fp) {
			fp = filepath.Join(baseDir, fp)
		}
		return fp
	}
	tlsPaths := func(cfg *config_util.TLSConfig) {
		cfg.CAFile = join(cfg.CAFile)
		cfg.CertFile = join(cfg.CertFile)
		cfg.KeyFile = join(cfg.KeyFile)
	}
	clientPaths := func(scfg *config_util.HTTPClientConfig) {
		if scfg.BasicAuth != nil {
			scfg.BasicAuth.PasswordFile = join(scfg.BasicAuth.PasswordFile)
		}
		scfg.BearerTokenFile = join(scfg.BearerTokenFile)
		tlsPaths(&scfg.TLSConfig)
	}
	for _, cfg := range cfg.RemoteReadConfigs {
		clientPaths(&cfg.HTTPClientConfig)
	}
}

type Config struct {
	RemoteReadConfigs []*RemoteReadConfig `yaml:"remote_read,omitempty"`
}

func (c Config) String() string {
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Sprintf("<error creating config string: %s>", err)
	}
	return string(b)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = Config{}
	// We want to set c to the defaults and then overwrite it with the input.
	// To make unmarshal fill the plain data struct rather than calling UnmarshalYAML
	// again, we have to hide it using a type indirection.
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	rrNames := map[string]struct{}{}
	for _, rrcfg := range c.RemoteReadConfigs {
		if rrcfg == nil {
			return errors.New("empty or null remote read config section")
		}
		// Skip empty names, we fill their name with their config hash in remote read code.
		if _, ok := rrNames[rrcfg.Name]; ok && rrcfg.Name != "" {
			return errors.Errorf("found multiple remote read configs with job name %q", rrcfg.Name)
		}
		rrNames[rrcfg.Name] = struct{}{}
	}
	return nil
}

type RemoteReadConfig struct {
	URL           *config_util.URL `yaml:"url"`
	RemoteTimeout model.Duration   `yaml:"remote_timeout,omitempty"`
	ReadRecent    bool             `yaml:"read_recent,omitempty"`
	Name          string           `yaml:"name,omitempty"`

	// We cannot do proper Go type embedding below as the parser will then parse
	// values arbitrarily into the overflow maps of further-down types.
	HTTPClientConfig config_util.HTTPClientConfig `yaml:",inline"`

	// RequiredMatchers is an optional list of equality matchers which have to
	// be present in a selector to query the remote read endpoint.
	RequiredMatchers model.LabelSet `yaml:"required_matchers,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *RemoteReadConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultRemoteReadConfig
	type plain RemoteReadConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.URL == nil {
		return errors.New("url for remote_read is empty")
	}
	// The UnmarshalYAML method of HTTPClientConfig is not being called because it's not a pointer.
	// We cannot make it a pointer as the parser panics for inlined pointer structs.
	// Thus we just do its validation here.
	return c.HTTPClientConfig.Validate()
}
