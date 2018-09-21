// Code generated by counterfeiter. DO NOT EDIT.
package oci_registryfakes

import (
	"sync"

	"github.com/cloudfoundry-incubator/bits-service/oci_registry"
)

type FakeAPIVersionManager struct {
	APIVersionStub        func(string, string) ([]byte, error)
	aPIVersionMutex       sync.RWMutex
	aPIVersionArgsForCall []struct {
		arg1 string
		arg2 string
	}
	aPIVersionReturns struct {
		result1 []byte
		result2 error
	}
	aPIVersionReturnsOnCall map[int]struct {
		result1 []byte
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeAPIVersionManager) APIVersion(arg1 string, arg2 string) ([]byte, error) {
	fake.aPIVersionMutex.Lock()
	ret, specificReturn := fake.aPIVersionReturnsOnCall[len(fake.aPIVersionArgsForCall)]
	fake.aPIVersionArgsForCall = append(fake.aPIVersionArgsForCall, struct {
		arg1 string
		arg2 string
	}{arg1, arg2})
	fake.recordInvocation("APIVersion", []interface{}{arg1, arg2})
	fake.aPIVersionMutex.Unlock()
	if fake.APIVersionStub != nil {
		return fake.APIVersionStub(arg1, arg2)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fake.aPIVersionReturns.result1, fake.aPIVersionReturns.result2
}

func (fake *FakeAPIVersionManager) APIVersionCallCount() int {
	fake.aPIVersionMutex.RLock()
	defer fake.aPIVersionMutex.RUnlock()
	return len(fake.aPIVersionArgsForCall)
}

func (fake *FakeAPIVersionManager) APIVersionArgsForCall(i int) (string, string) {
	fake.aPIVersionMutex.RLock()
	defer fake.aPIVersionMutex.RUnlock()
	return fake.aPIVersionArgsForCall[i].arg1, fake.aPIVersionArgsForCall[i].arg2
}

func (fake *FakeAPIVersionManager) APIVersionReturns(result1 []byte, result2 error) {
	fake.APIVersionStub = nil
	fake.aPIVersionReturns = struct {
		result1 []byte
		result2 error
	}{result1, result2}
}

func (fake *FakeAPIVersionManager) APIVersionReturnsOnCall(i int, result1 []byte, result2 error) {
	fake.APIVersionStub = nil
	if fake.aPIVersionReturnsOnCall == nil {
		fake.aPIVersionReturnsOnCall = make(map[int]struct {
			result1 []byte
			result2 error
		})
	}
	fake.aPIVersionReturnsOnCall[i] = struct {
		result1 []byte
		result2 error
	}{result1, result2}
}

func (fake *FakeAPIVersionManager) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.aPIVersionMutex.RLock()
	defer fake.aPIVersionMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeAPIVersionManager) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ oci_registry.APIVersionManager = new(FakeAPIVersionManager)
