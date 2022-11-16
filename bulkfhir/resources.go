package bulkfhir

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	apb "github.com/google/fhir/go/proto/google/fhir/proto/annotations_go_proto"
	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

var (
	resourceTypeCodeEnumToName = map[cpb.ResourceTypeCode_Value]string{}
	resourceTypeCodeNameToEnum = map[string]cpb.ResourceTypeCode_Value{}
	once                       = sync.Once{}
)

func computeMaps() error {
	var err error
	once.Do(func() {
		values := cpb.ResourceTypeCode_INVALID_UNINITIALIZED.Descriptor().Values()
		for i := 0; i < values.Len(); i++ {
			val := values.Get(i)
			enum := cpb.ResourceTypeCode_Value(val.Number())
			if enum == cpb.ResourceTypeCode_INVALID_UNINITIALIZED {
				continue
			}
			opts := val.Options()
			if !proto.HasExtension(opts, apb.E_FhirOriginalCode) {
				// Other than INVALID_UNINITIALIZED, all of the values in the
				// ResourceTypeCode.Value enum should have the fhir_original_code option
				// set.
				err = fmt.Errorf("ResourceTypeCode.Value.%s has no fhir_original_code option", enum)
				return
			}
			name := proto.GetExtension(opts, apb.E_FhirOriginalCode).(string)
			resourceTypeCodeNameToEnum[name] = enum
			resourceTypeCodeEnumToName[enum] = name
		}
	})
	return err
}

// ResourceTypeCodeToName returns the FHIR resource name corresponding to the
// ResourceTypeCode.
func ResourceTypeCodeToName(val cpb.ResourceTypeCode_Value) (string, error) {
	if err := computeMaps(); err != nil {
		return "", err
	}
	if name, ok := resourceTypeCodeEnumToName[val]; ok {
		return name, nil
	}
	return "", fmt.Errorf("ResourceTypeCode %s does not have a FHIR resource name", val)
}

// ResourceTypeCodeFromName returns the ResourceTypeCode for the given enum.
func ResourceTypeCodeFromName(name string) (cpb.ResourceTypeCode_Value, error) {
	if err := computeMaps(); err != nil {
		return cpb.ResourceTypeCode_INVALID_UNINITIALIZED, err
	}
	if enum, ok := resourceTypeCodeNameToEnum[name]; ok {
		return enum, nil
	}
	return cpb.ResourceTypeCode_INVALID_UNINITIALIZED, fmt.Errorf("could not find a ResourceTypeCode with FHIR resource name %q", name)
}
