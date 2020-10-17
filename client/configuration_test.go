package client_test

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionStringWithNoSchema(t *testing.T) {
	config, err := client.ParseConnectionString(":so/mething/random")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "scheme is missing")
}

func TestConnectionStringWithInvalidScheme(t *testing.T) {
	config, err := client.ParseConnectionString("esdbwrong://")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid scheme")
}

func TestConnectionStringWithInvalidUserCredentials(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "user credentials are invalid")

	config, err = client.ParseConnectionString("esdb://us:er:pa:ss@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "user credentials are invalid")

	config, err = client.ParseConnectionString("esdb://:pass@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username is empty")

	config, err = client.ParseConnectionString("esdb://user:@127.0.0.1/")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "password is empty")
}

func TestConnectionStringWithInvalidHost(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "empty host")

	config, err = client.ParseConnectionString("esdb://user:pass@")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "empty host")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:abc")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:1234,127.0.0.2:abc,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:abc:def")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:abc:def")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@localhost:1234,127.0.0.2:abc:def,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@localhost:1234,,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "empty host")
}

func TestConnectionStringWithEmptyPath(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:1234")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1?maxDiscoverAttempts=10")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=10")
	assert.Nil(t, err)
	assert.NotNil(t, config)
}

func TestConnectionStringWithNonEmptyPath(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/test")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/maxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/hello?maxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")
}

func TestConnectionStringWithDuplicateKey(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=1234&MaxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "duplicate key/value pair")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?gossipTimeout=10&gossipTimeout=30")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "duplicate key/value pair")
}

func TestConnectionStringWithoutSettings(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?")
	assert.Nil(t, err)
	assert.NotNil(t, config)
}

func TestConnectionStringWithInvalidKeyValuePair(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=12=34")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid key/value pair specified")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts1234")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid key/value pair specified")
}

func TestConnectionStringWithInvalidSettings(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/?unknown=1234")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Unknown setting")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "No value specified for")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=1234&hello=test")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Unknown setting")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=abcd")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?discoveryInterval=abcd")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?gossipTimeout=defg")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?tlsVerifyCert=truee")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be either true or false")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=blabla")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid NodePreference")
}

func TestConnectionStringWithDifferentNodePreferences(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=leader")
	assert.NoError(t, err)
	assert.Equal(t, client.NodePreference_Leader, config.NodePreference)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=Follower")
	assert.NoError(t, err)
	assert.Equal(t, client.NodePreference_Follower, config.NodePreference)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=rAndom")
	assert.NoError(t, err)
	assert.Equal(t, client.NodePreference_Random, config.NodePreference)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=ReadOnlyReplica")
	assert.NoError(t, err)
	assert.Equal(t, client.NodePreference_ReadOnlyReplica, config.NodePreference)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=invalid")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid NodePreference")
}

func TestConnectionStringWithValidSingleNodeConnectionString(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=13&DiscoveryInterval=37&gossipTimeout=33&nOdEPrEfErence=FoLLoWer&tlsVerifyCert=faLse")
	assert.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Equal(t, "https://127.0.0.1:2113", config.Address)
	assert.Empty(t, config.GossipSeeds)
	assert.Equal(t, 13, config.MaxDiscoverAttempts)
	assert.Equal(t, 37, config.DiscoveryInterval)
	assert.Equal(t, 33, config.GossipTimeout)
	assert.Equal(t, client.NodePreference_Follower, config.NodePreference)
	assert.Equal(t, true, config.SkipCertificateVerification)

	config, err = client.ParseConnectionString("esdb://hostname:4321/?tls=fAlse")
	assert.NoError(t, err)
	assert.Empty(t, config.Username)
	assert.Empty(t, config.Password)
	assert.Equal(t, "http://hostname:4321", config.Address)
	assert.Empty(t, config.GossipSeeds)
	assert.Empty(t, config.NodePreference)
	assert.Equal(t, false, config.UseTls)
	assert.Equal(t, false, config.SkipCertificateVerification)
}

func TestConnectionStringWithValidClusterConnectionString(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1,127.0.0.2:3321,127.0.0.3/?maxDiscoverAttempts=13&DiscoveryInterval=37&nOdEPrEfErence=FoLLoWer&tlsVerifyCert=false")
	assert.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Empty(t, config.Address)
	assert.Equal(t, 13, config.MaxDiscoverAttempts)
	assert.Equal(t, 37, config.DiscoveryInterval)
	assert.Equal(t, client.NodePreference_Follower, config.NodePreference)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, "https://127.0.0.1:2113", config.GossipSeeds[0])
	assert.Equal(t, "https://127.0.0.2:3321", config.GossipSeeds[1])
	assert.Equal(t, "https://127.0.0.3:2113", config.GossipSeeds[2])

	config, err = client.ParseConnectionString("esdb://user:pass@host1,host2:3321,127.0.0.3/?tls=false&maxDiscoverAttempts=13&DiscoveryInterval=37&nOdEPrEfErence=FoLLoWer&tlsVerifyCert=false")
	assert.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Empty(t, config.Address)
	assert.Equal(t, 13, config.MaxDiscoverAttempts)
	assert.Equal(t, 37, config.DiscoveryInterval)
	assert.Equal(t, client.NodePreference_Follower, config.NodePreference)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, "http://host1:2113", config.GossipSeeds[0])
	assert.Equal(t, "http://host2:3321", config.GossipSeeds[1])
	assert.Equal(t, "http://127.0.0.3:2113", config.GossipSeeds[2])
}
