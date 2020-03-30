package config

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	influxtesting "github.com/influxdata/influxdb/testing"
)

func TestWriteConfigs(t *testing.T) {
	cases := []struct {
		name   string
		err    *influxdb.Error
		pp     Configs
		result string
	}{
		{
			name: "bad name -",
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `"-" is not a valid config name`,
			},
			pp: Configs{
				"-": Config{},
			},
		},
		{
			name: "new config",
			pp: Configs{
				"default": Config{
					Token:  "token1",
					Org:    "org1",
					Host:   "http://localhost:9999",
					Active: true,
				},
			},
			result: `[default]
  url = "http://localhost:9999"
  token = "token1"
  org = "org1"
  active = true` + commentedStr,
		},
		{
			name: "multiple",
			pp: Configs{
				"config1": Config{
					Token: "token1",
					Host:  "host1",
				},
				"config2": Config{
					Token:  "token2",
					Host:   "host2",
					Org:    "org2",
					Active: true,
				},
				"config3": Config{
					Token: "token3",
					Host:  "host3",
					Org:   "org3",
				},
			},
			result: `[config1]
  url = "host1"
  token = "token1"
  org = ""

[config2]
  url = "host2"
  token = "token2"
  org = "org2"
  active = true

[config3]
  url = "host3"
  token = "token3"
  org = "org3"` + commentedStr,
		},
	}
	for _, c := range cases {
		var b1 bytes.Buffer
		err := writeConfigs(c.pp, &b1)
		influxtesting.ErrorsEqual(t, err, err)
		if c.err == nil {
			if diff := cmp.Diff(c.result, b1.String()); diff != "" {
				t.Fatalf("write configs %s err, diff %s", c.name, diff)
			}
		}
	}
}

var commentedStr = `
# 
# [eu-central]
#   url = "https://eu-central-1-1.aws.cloud2.influxdata.com"
#   token = "XXX"
#   org = ""
# 
# [us-central]
#   url = "https://us-central1-1.gcp.cloud2.influxdata.com"
#   token = "XXX"
#   org = ""
# 
# [us-west]
#   url = "https://us-west-2-1.aws.cloud2.influxdata.com"
#   token = "XXX"
#   org = ""
`

func TestParseActiveConfig(t *testing.T) {
	cases := []struct {
		name   string
		hasErr bool
		src    string
		p      Config
	}{
		{
			name:   "bad src",
			src:    "bad [toml",
			hasErr: true,
		},
		{
			name:   "nothing",
			hasErr: true,
		},
		{
			name:   "conflicted",
			hasErr: true,
			src: `
			[a1]
			url = "host1"
			active =true
			[a2]
			url = "host2"
			active = true			
			`,
		},
		{
			name:   "one active",
			hasErr: false,
			src: `
			[a1]
			url = "host1"
			[a2]
			url = "host2"
			active = true
			[a3]
			url = "host3"
			[a4]
			url = "host4"				
			`,
			p: Config{
				Name:   "a2",
				Host:   "host2",
				Active: true,
			},
		},
	}
	for _, c := range cases {
		r := bytes.NewBufferString(c.src)
		p, err := ParseActiveConfig(r)
		if c.hasErr {
			if err == nil {
				t.Fatalf("parse active config %q failed, should have error, got nil", c.name)
			}
			continue
		}
		if diff := cmp.Diff(p, c.p); diff != "" {
			t.Fatalf("parse active config %s failed, diff %s", c.name, diff)
		}
	}
}

func TestParsePreviousActiveConfig(t *testing.T) {
	cases := []struct {
		name   string
		hasErr bool
		src    string
		p      Config
	}{
		{
			name:   "bad src",
			src:    "bad [toml",
			hasErr: true,
		},
		{
			name:   "nothing",
			hasErr: true,
		},
		{
			name:   "conflicted",
			hasErr: true,
			src: `
			[a1]
			url = "host1"
			previous =true
			[a2]
			url = "host2"
			previous = true			
			`,
		},
		{
			name:   "one previous active",
			hasErr: false,
			src: `
			[a1]
			url = "host1"
			[a2]
			url = "host2"
			previous = true
			[a3]
			url = "host3"
			[a4]
			url = "host4"				
			`,
			p: Config{
				Name:           "a2",
				Host:           "host2",
				PreviousActive: true,
			},
		},
	}
	for _, c := range cases {
		r := bytes.NewBufferString(c.src)
		p, err := ParsePreviousActive(r)
		if c.hasErr {
			if err == nil {
				t.Fatalf("parse previous active config %q failed, should have error, got nil", c.name)
			}
			continue
		}
		if diff := cmp.Diff(p, c.p); diff != "" {
			t.Fatalf("parse previous active config %s failed, diff %s", c.name, diff)
		}
	}
}

func TestConfigsSwith(t *testing.T) {
	cases := []struct {
		name   string
		old    Configs
		new    Configs
		target string
		err    error
	}{
		{
			name:   "not found",
			target: "p1",
			old: Configs{
				"a1": {Host: "host1"},
				"a2": {Host: "host2"},
			},
			new: Configs{
				"a1": {Host: "host1"},
				"a2": {Host: "host2"},
			},
			err: &influxdb.Error{
				Code: influxdb.ENotFound,
				Msg:  `config "p1" is not found`,
			},
		},
		{
			name:   "regular switch",
			target: "a1",
			old: Configs{
				"a1": {Host: "host1"},
				"a2": {Host: "host2"},
				"a3": {Host: "host3", Active: true},
			},
			new: Configs{
				"a1": {Host: "host1", Active: true},
				"a2": {Host: "host2"},
				"a3": {Host: "host3", PreviousActive: true},
			},
			err: nil,
		},
	}
	for _, c := range cases {
		err := c.old.Switch(c.target)
		influxtesting.ErrorsEqual(t, err, c.err)
		if diff := cmp.Diff(c.old, c.new); diff != "" {
			t.Fatalf("switch config %s failed, diff %s", c.name, diff)
		}
	}
}
