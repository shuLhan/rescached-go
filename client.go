// SPDX-FileCopyrightText: 2022 M. Shulhan <ms@kilabit.info>
// SPDX-License-Identifier: GPL-3.0-or-later

package rescached

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/shuLhan/share/lib/dns"
	libhttp "github.com/shuLhan/share/lib/http"
)

// Client for rescached.
type Client struct {
	*libhttp.Client
}

// NewClient create new HTTP client that connect to rescached HTTP server.
func NewClient(serverUrl string, insecure bool) (cl *Client) {
	var (
		httpcOpts = libhttp.ClientOptions{
			ServerUrl:     serverUrl,
			AllowInsecure: insecure,
		}
	)
	cl = &Client{
		Client: libhttp.NewClient(&httpcOpts),
	}
	return cl
}

// BlockdDisable disable specific hosts on block.d.
func (cl *Client) BlockdDisable(blockdName string) (an interface{}, err error) {
	var (
		logp   = "BlockdDisable"
		res    = libhttp.EndpointResponse{}
		params = url.Values{}

		hb   *hostsBlock
		resb []byte
	)

	params.Set(paramNameName, blockdName)

	_, resb, err = cl.PostForm(apiBlockdDisable, nil, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	res.Data = &hb
	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}

	return hb, nil
}

// BlockdEnable enable specific hosts on block.d.
func (cl *Client) BlockdEnable(blockdName string) (an interface{}, err error) {
	var (
		logp   = "BlockdEnable"
		res    = libhttp.EndpointResponse{}
		params = url.Values{}

		hb   *hostsBlock
		resb []byte
	)

	params.Set(paramNameName, blockdName)

	_, resb, err = cl.PostForm(apiBlockdEnable, nil, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	res.Data = &hb
	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}

	return hb, nil
}

// BlockdUpdate fetch the latest hosts file from the hosts block
// provider based on registered URL.
func (cl *Client) BlockdUpdate(blockdName string) (an interface{}, err error) {
	var (
		logp   = "BlockdUpdate"
		params = url.Values{}
		res    = libhttp.EndpointResponse{}

		hb   *hostsBlock
		resb []byte
	)

	params.Set(paramNameName, blockdName)

	_, resb, err = cl.PostForm(apiBlockdUpdate, nil, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	res.Data = &hb
	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}

	return hb, nil
}

// Caches fetch all of non-local caches from server.
func (cl *Client) Caches() (answers []*dns.Answer, err error) {
	var (
		logp = "Caches"
		res  = libhttp.EndpointResponse{
			Data: &answers,
		}

		resb []byte
	)

	_, resb, err = cl.Get(apiCaches, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}

	return answers, nil
}

func (cl *Client) CachesRemove(q string) (listAnswer []*dns.Answer, err error) {
	var (
		logp   = "CachesRemove"
		params = url.Values{}
		res    = libhttp.EndpointResponse{
			Data: &listAnswer,
		}

		resb []byte
	)

	params.Set(paramNameName, q)

	_, resb, err = cl.Delete(apiCaches, nil, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}

	return listAnswer, nil
}

// CachesSearch search the answer in caches by its domain name and return it
// as DNS message.
func (cl *Client) CachesSearch(q string) (listMsg []*dns.Message, err error) {
	var (
		logp   = "CachesSearch"
		params = url.Values{}
		res    = libhttp.EndpointResponse{
			Data: &listMsg,
		}

		resb []byte
	)

	params.Set(paramNameQuery, q)

	_, resb, err = cl.Get(apiCachesSearch, nil, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}

	return listMsg, nil
}

// Env get the server environment.
func (cl *Client) Env() (env *Environment, err error) {
	var (
		logp = "Env"
		res  = libhttp.EndpointResponse{
			Data: &env,
		}
		resb []byte
	)

	_, resb, err = cl.Get(apiEnvironment, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}
	return env, nil
}

// EnvUpdate update the server environment using new Environment.
func (cl *Client) EnvUpdate(envIn *Environment) (envOut *Environment, err error) {
	var (
		logp = "EnvUpdate"
		res  = libhttp.EndpointResponse{
			Data: &envOut,
		}

		resb []byte
	)

	_, resb, err = cl.PostJSON(apiEnvironment, nil, envIn)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}
	return envOut, nil
}

// HostsdCreate create new hosts file inside the hosts.d with requested name.
func (cl *Client) HostsdCreate(name string) (hostsFile *dns.HostsFile, err error) {
	var (
		logp = "HostsdCreate"
		res  = libhttp.EndpointResponse{
			Data: &hostsFile,
		}
		params = url.Values{}

		resb []byte
	)

	params.Set(paramNameName, name)

	_, resb, err = cl.PostForm(apiHostsd, nil, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}
	return hostsFile, nil
}

// HostsdDelete delete hosts file inside the hosts.d by file name.
func (cl *Client) HostsdDelete(name string) (hostsFile *dns.HostsFile, err error) {
	var (
		logp = "HostsdDelete"
		res  = libhttp.EndpointResponse{
			Data: &hostsFile,
		}
		params = url.Values{}

		resb []byte
	)

	params.Set(paramNameName, name)

	_, resb, err = cl.Delete(apiHostsd, nil, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}
	return hostsFile, nil
}

// HostsdGet get the content of hosts file inside the hosts.d by file name.
func (cl *Client) HostsdGet(name string) (listrr []*dns.ResourceRecord, err error) {
	var (
		logp = "HostsdGet"
		res  = libhttp.EndpointResponse{
			Data: &listrr,
		}
		params = url.Values{}

		resb []byte
	)

	params.Set(paramNameName, name)

	_, resb, err = cl.Get(apiHostsd, nil, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}

	err = json.Unmarshal(resb, &res)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", logp, err)
	}
	if res.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", logp, res.Code, res.Message)
	}
	return listrr, nil
}
