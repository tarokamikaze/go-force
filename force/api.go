package force

import (
	"errors"
	"fmt"
	"net/url"
)

const (
	limitsKey          = "limits"
	queryKey           = "query"
	queryAllKey        = "queryAll"
	sObjectsKey        = "sobjects"
	sObjectKey         = "sobject"
	sObjectDescribeKey = "describe"

	rowTemplateKey = "rowTemplate"
	idKey          = "{ID}"

	resourcesUri = "/services/data/%v/"
	versionsUri  = "/services/data"
)

type ForceApiInterface interface {
	BulkInsertSObjects(table string, in []SObject) ([]*SObjectResponse, error)
	BulkUpdateSObjects(table string, in []SObject) ([]*SObjectResponse, error)
	Delete(path string, params url.Values) error
	DeleteSObject(id string, in SObject) (err error)
	DeleteSObjectByExternalId(id string, in SObject) (err error)
	DescribeSObject(in SObject) (resp *SObjectDescription, err error)
	DescribeSObjects() (map[string]*SObjectMetaData, error)
	Get(path string, params url.Values, out interface{}) error
	GetAccessToken() string
	GetInstanceURL() string
	GetLimits() (limits *Limits, err error)
	GetSObject(id string, fields []string, out SObject) (err error)
	GetSObjectByExternalId(id string, fields []string, out SObject) (err error)
	InsertSObject(in SObject) (resp *SObjectResponse, err error)
	Patch(path string, params url.Values, payload, out interface{}) error
	Post(path string, params url.Values, payload, out interface{}) error
	Put(path string, params url.Values, payload, out interface{}) error
	Query(query string, out interface{}) (err error)
	QueryAll(query string, out interface{}) (err error)
	QueryNext(uri string, out interface{}) (err error)
	RefreshToken() error
	TraceOff()
	TraceOn(prefix string, logger ForceApiLogger)
	UpdateSObject(id string, in SObject) (err error)
	UpsertSObjectByExternalId(id string, in SObject) (resp *SObjectResponse, err error)
}

type ForceApi struct {
	OAuth                  *ForceOauth
	apiVersion             string
	apiVersions            []*Version
	apiResources           map[string]string
	apiSObjects            map[string]*SObjectMetaData
	apiSObjectDescriptions map[string]*SObjectDescription
	apiMaxBatchSize        int64
	logger                 ForceApiLogger
	logPrefix              string
}

type Version struct {
	Label   string `json:"label"`
	URL     string `json:"url"`
	Version string `json:"version"`
}

type RefreshTokenResponse struct {
	ID          string `json:"id"`
	IssuedAt    string `json:"issued_at"`
	Signature   string `json:"signature"`
	AccessToken string `json:"access_token"`
}

type SObjectApiResponse struct {
	Encoding     string             `json:"encoding"`
	MaxBatchSize int64              `json:"maxBatchSize"`
	SObjects     []*SObjectMetaData `json:"sobjects"`
}

type SObjectMetaData struct {
	Name                string            `json:"name"`
	Label               string            `json:"label"`
	KeyPrefix           string            `json:"keyPrefix"`
	LabelPlural         string            `json:"labelPlural"`
	Custom              bool              `json:"custom"`
	Layoutable          bool              `json:"layoutable"`
	Activateable        bool              `json:"activateable"`
	URLs                map[string]string `json:"urls"`
	Searchable          bool              `json:"searchable"`
	Updateable          bool              `json:"updateable"`
	Createable          bool              `json:"createable"`
	DeprecatedAndHidden bool              `json:"deprecatedAndHidden"`
	CustomSetting       bool              `json:"customSetting"`
	Deletable           bool              `json:"deletable"`
	FeedEnabled         bool              `json:"feedEnabled"`
	Mergeable           bool              `json:"mergeable"`
	Queryable           bool              `json:"queryable"`
	Replicateable       bool              `json:"replicateable"`
	Retrieveable        bool              `json:"retrieveable"`
	Undeletable         bool              `json:"undeletable"`
	Triggerable         bool              `json:"triggerable"`
}

type SObjectDescription struct {
	Name                string               `json:"name"`
	Fields              []*SObjectField      `json:"fields"`
	KeyPrefix           string               `json:"keyPrefix"`
	Layoutable          bool                 `json:"layoutable"`
	Activateable        bool                 `json:"activateable"`
	LabelPlural         string               `json:"labelPlural"`
	Custom              bool                 `json:"custom"`
	CompactLayoutable   bool                 `json:"compactLayoutable"`
	Label               string               `json:"label"`
	Searchable          bool                 `json:"searchable"`
	URLs                map[string]string    `json:"urls"`
	Queryable           bool                 `json:"queryable"`
	Deletable           bool                 `json:"deletable"`
	Updateable          bool                 `json:"updateable"`
	Createable          bool                 `json:"createable"`
	CustomSetting       bool                 `json:"customSetting"`
	Undeletable         bool                 `json:"undeletable"`
	Mergeable           bool                 `json:"mergeable"`
	Replicateable       bool                 `json:"replicateable"`
	Triggerable         bool                 `json:"triggerable"`
	FeedEnabled         bool                 `json:"feedEnabled"`
	Retrievable         bool                 `json:"retrieveable"`
	SearchLayoutable    bool                 `json:"searchLayoutable"`
	LookupLayoutable    bool                 `json:"lookupLayoutable"`
	Listviewable        bool                 `json:"listviewable"`
	DeprecatedAndHidden bool                 `json:"deprecatedAndHidden"`
	RecordTypeInfos     []*RecordTypeInfo    `json:"recordTypeInfos"`
	ChildRelationsips   []*ChildRelationship `json:"childRelationships"`

	AllFields string `json:"-"` // Not from force.com API. Used to generate SELECT * queries.
}

type SObjectField struct {
	Length                   float64          `json:"length"`
	Name                     string           `json:"name"`
	Type                     string           `json:"type"`
	DefaultValue             interface{}      `json:"defaultValue"`
	RestrictedPicklist       bool             `json:"restrictedPicklist"`
	NameField                bool             `json:"nameField"`
	ByteLength               float64          `json:"byteLength"`
	Precision                float64          `json:"precision"`
	Filterable               bool             `json:"filterable"`
	Sortable                 bool             `json:"sortable"`
	Unique                   bool             `json:"unique"`
	CaseSensitive            bool             `json:"caseSensitive"`
	Calculated               bool             `json:"calculated"`
	Scale                    float64          `json:"scale"`
	Label                    string           `json:"label"`
	NamePointing             bool             `json:"namePointing"`
	Custom                   bool             `json:"custom"`
	HtmlFormatted            bool             `json:"htmlFormatted"`
	DependentPicklist        bool             `json:"dependentPicklist"`
	Permissionable           bool             `json:"permissionable"`
	ReferenceTo              []string         `json:"referenceTo"`
	RelationshipOrder        float64          `json:"relationshipOrder"`
	SoapType                 string           `json:"soapType"`
	CalculatedValueFormula   string           `json:"calculatedValueFormula"`
	DefaultValueFormula      string           `json:"defaultValueFormula"`
	DefaultedOnCreate        bool             `json:"defaultedOnCreate"`
	Digits                   float64          `json:"digits"`
	Groupable                bool             `json:"groupable"`
	Nillable                 bool             `json:"nillable"`
	InlineHelpText           string           `json:"inlineHelpText"`
	WriteRequiresMasterRead  bool             `json:"writeRequiresMasterRead"`
	PicklistValues           []*PicklistValue `json:"picklistValues"`
	Updateable               bool             `json:"updateable"`
	Createable               bool             `json:"createable"`
	DeprecatedAndHidden      bool             `json:"deprecatedAndHidden"`
	DisplayLocationInDecimal bool             `json:"displayLocationInDecimal"`
	CascadeDelete            bool             `json:"cascasdeDelete"`
	RestrictedDelete         bool             `json:"restrictedDelete"`
	ControllerName           string           `json:"controllerName"`
	ExternalId               bool             `json:"externalId"`
	IdLookup                 bool             `json:"idLookup"`
	AutoNumber               bool             `json:"autoNumber"`
	RelationshipName         string           `json:"relationshipName"`
}

type PicklistValue struct {
	Value       string `json:"value"`
	DefaulValue bool   `json:"defaultValue"`
	ValidFor    string `json:"validFor"`
	Active      bool   `json:"active"`
	Label       string `json:"label"`
}

type RecordTypeInfo struct {
	Name                     string            `json:"name"`
	Available                bool              `json:"available"`
	RecordTypeId             string            `json:"recordTypeId"`
	URLs                     map[string]string `json:"urls"`
	DefaultRecordTypeMapping bool              `json:"defaultRecordTypeMapping"`
}

type ChildRelationship struct {
	Field               string `json:"field"`
	ChildSObject        string `json:"childSObject"`
	DeprecatedAndHidden bool   `json:"deprecatedAndHidden"`
	CascadeDelete       bool   `json:"cascadeDelete"`
	RestrictedDelete    bool   `json:"restrictedDelete"`
	RelationshipName    string `json:"relationshipName"`
}

func (forceApi *ForceApi) GetApiSObjectDescription(name string) (*SObjectDescription, error) {
	if desc, ok := forceApi.apiSObjectDescriptions[name]; ok {
		return desc, nil
	} else {
		if sObject, ok := forceApi.apiSObjects[name]; ok {
			uri := sObject.URLs[sObjectDescribeKey]

			desc := &SObjectDescription{}
			err := forceApi.Get(uri, nil, desc)
			if err != nil {
				return nil, err
			}

			forceApi.apiSObjectDescriptions[name] = desc
			return desc, nil
		} else {
			return nil, errors.New("Not found")
		}
	}
}

func (forceApi *ForceApi) getApiVersions() error {
	return forceApi.Get(versionsUri, nil, &forceApi.apiVersions)
}

func (forceApi *ForceApi) getApiResources() error {
	uri := fmt.Sprintf(resourcesUri, forceApi.apiVersion)
	return forceApi.Get(uri, nil, &forceApi.apiResources)
}

func (forceApi *ForceApi) getApiSObjects() error {
	uri := forceApi.apiResources[sObjectsKey]

	list := &SObjectApiResponse{}
	err := forceApi.Get(uri, nil, list)
	if err != nil {
		return err
	}

	forceApi.apiMaxBatchSize = list.MaxBatchSize

	// The API doesn't return the list of sobjects in a map. Convert it.
	for _, object := range list.SObjects {
		forceApi.apiSObjects[object.Name] = object
	}

	return nil
}

func (forceApi *ForceApi) getApiSObjectDescriptions() error {
	for name, metaData := range forceApi.apiSObjects {
		uri := metaData.URLs[sObjectDescribeKey]

		desc := &SObjectDescription{}
		err := forceApi.Get(uri, nil, desc)
		if err != nil {
			return err
		}

		forceApi.apiSObjectDescriptions[name] = desc
	}

	return nil
}

func (forceApi *ForceApi) GetInstanceURL() string {
	return forceApi.OAuth.InstanceUrl
}

func (forceApi *ForceApi) GetAccessToken() string {
	return forceApi.OAuth.AccessToken
}

func (forceApi *ForceApi) RefreshToken() error {
	res := &RefreshTokenResponse{}
	payload := map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": forceApi.OAuth.refreshToken,
		"client_id":     forceApi.OAuth.clientId,
		"client_secret": forceApi.OAuth.clientSecret,
	}

	err := forceApi.Post("/services/oauth2/token", nil, payload, res)
	if err != nil {
		return err
	}

	forceApi.OAuth.AccessToken = res.AccessToken
	return nil
}
