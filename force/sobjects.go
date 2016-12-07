package force

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Interface all standard and custom objects must implement. Needed for uri generation.
type SObject interface {
	SetID(string)
	ApiName() string
	ExternalIdApiName() string
}

// Response recieved from force.com API after insert of an sobject.
type SObjectResponse struct {
	Id      string         `json:"id,omitempty"`
	Errors  []SObjectError `json:"errors,omitempty"` //TODO: Not sure if ApiErrors is the right object
	Success bool           `json:"success,omitempty"`
}
type SObjectError struct {
	Message              string      `json:"message"`
	Fields               []string    `json:"fields"`
	StatusCode           string      `json:"statusCode"`
	ExtendedErrorDetails interface{} `json:"extendedErrorDetails"`
}

// Response recieved from force.com API after insert of an sobject.
type CreateJobRequest struct {
	Operation   string `json:"operation,omitempty"`
	Object      string `json:"object,omitempty"`
	ContentType string `json:"contentType,omitempty"`
}

// Response recieved from force.com API after insert of an sobject.
type CloseJobRequest struct {
	State string `json:"state,omitempty"`
}

type CreateJobResponse struct {
	ApexProcessingTime      int     `json:"apexProcessingTime"`
	APIActiveProcessingTime int     `json:"apiActiveProcessingTime"`
	APIVersion              float64 `json:"apiVersion"`
	ConcurrencyMode         string  `json:"concurrencyMode"`
	ContentType             string  `json:"contentType"`
	CreatedByID             string  `json:"createdById"`
	CreatedDate             string  `json:"createdDate"`
	ID                      string  `json:"id"`
	NumberBatchesCompleted  int     `json:"numberBatchesCompleted"`
	NumberBatchesFailed     int     `json:"numberBatchesFailed"`
	NumberBatchesInProgress int     `json:"numberBatchesInProgress"`
	NumberBatchesQueued     int     `json:"numberBatchesQueued"`
	NumberBatchesTotal      int     `json:"numberBatchesTotal"`
	NumberRecordsFailed     int     `json:"numberRecordsFailed"`
	NumberRecordsProcessed  int     `json:"numberRecordsProcessed"`
	NumberRetries           int     `json:"numberRetries"`
	Object                  string  `json:"object"`
	Operation               string  `json:"operation"`
	State                   string  `json:"state"`
	SystemModstamp          string  `json:"systemModstamp"`
	TotalProcessingTime     int     `json:"totalProcessingTime"`
}

type CreateBatchResponse struct {
	ApexProcessingTime      int    `json:"apexProcessingTime"`
	APIActiveProcessingTime int    `json:"apiActiveProcessingTime"`
	CreatedDate             string `json:"createdDate"`
	ID                      string `json:"id"`
	JobID                   string `json:"jobId"`
	NumberRecordsFailed     int    `json:"numberRecordsFailed"`
	NumberRecordsProcessed  int    `json:"numberRecordsProcessed"`
	State                   string `json:"state"`
	SystemModstamp          string `json:"systemModstamp"`
	TotalProcessingTime     int    `json:"totalProcessingTime"`
}

func (forceApi *ForceApi) DescribeSObject(in SObject) (resp *SObjectDescription, err error) {
	// Check cache
	resp, ok := forceApi.apiSObjectDescriptions[in.ApiName()]
	if !ok {
		// Attempt retrieval from api
		sObjectMetaData, ok := forceApi.apiSObjects[in.ApiName()]
		if !ok {
			err = fmt.Errorf("Unable to find metadata for object: %v", in.ApiName())
			return
		}

		uri := sObjectMetaData.URLs[sObjectDescribeKey]

		resp = &SObjectDescription{}
		err = forceApi.Get(uri, nil, resp)
		if err != nil {
			return
		}

		// Create Comma Separated String of All Field Names.
		// Used for SELECT * Queries.
		length := len(resp.Fields)
		if length > 0 {
			var allFields bytes.Buffer
			for index, field := range resp.Fields {
				// Field type location cannot be directly retrieved from SQL Query.
				if field.Type != "location" {
					if index > 0 && index < length {
						allFields.WriteString(", ")
					}
					allFields.WriteString(field.Name)
				}
			}

			resp.AllFields = allFields.String()
		}

		forceApi.apiSObjectDescriptions[in.ApiName()] = resp
	}

	return
}

// Get a list of all object types
func (forceApi *ForceApi) GetSObjects() (map[string]*SObjectMetaData, error) {
	return forceApi.apiSObjects, nil
}

func (forceApi *ForceApi) GetSObject(id string, fields []string, out SObject) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[out.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	params := url.Values{}
	if len(fields) > 0 {
		params.Add("fields", strings.Join(fields, ","))
	}

	err = forceApi.Get(uri, params, out.(interface{}))

	return
}

func (forceApi *ForceApi) BulkQuerySObjects(table string, query string) ([]*SObjectResponse, error) {
	if _, ok := forceApi.apiSObjects[table]; ok {

		job, err := forceApi.createJob(table, "query", "CSV")

		if nil != err {
			return nil, err
		}

		defer forceApi.closeJob(job.ID)

		res, err := forceApi.createQueryBatch(job.ID, query)

		if nil != err {
			return nil, err
		}

		batchID := res.ID

		for res.State != "Completed" {
			time.Sleep(time.Second * time.Duration(2))
			res, err = forceApi.getBatchStatus(job.ID, batchID)

			if nil != err {
				return nil, err
			}

			if res.State == "Failed" {
				return nil, errors.New("Failed import")
			}
		}

		if res.State == "Completed" {
			results, err := forceApi.getBatchResults(job.ID, batchID)
			if nil != err {
				return nil, err
			}

			return results, nil
		}

		return nil, errors.New("Unknown error")

	} else {
		err := errors.New("Not found")

		return nil, err
	}
}

func (forceApi *ForceApi) BulkInsertSObjects(table string, in []SObject) ([]*SObjectResponse, error) {
	if _, ok := forceApi.apiSObjects[table]; ok {

		job, err := forceApi.createJob(table, "insert", "JSON")

		if nil != err {
			return nil, err
		}

		defer forceApi.closeJob(job.ID)

		res, err := forceApi.createInsertBatch(job.ID, in)

		if nil != err {
			return nil, err
		}

		batchID := res.ID

		for res.State != "Completed" {
			time.Sleep(time.Second * time.Duration(2))
			res, err = forceApi.getBatchStatus(job.ID, batchID)

			if nil != err {
				return nil, err
			}

			if res.State == "Failed" {
				return nil, errors.New("Failed import")
			}
		}

		if res.State == "Completed" {
			results, err := forceApi.getBatchResults(job.ID, batchID)
			if nil != err {
				return nil, err
			}

			return results, nil
		}

		return nil, errors.New("Unknown error")

	} else {
		err := errors.New("Not found")

		return nil, err
	}
}

func (forceApi *ForceApi) createQueryBatch(jobID string, query string) (*CreateBatchResponse, error) {

	jobResp := &CreateBatchResponse{}
	err := forceApi.requestWithContentType("POST", "/services/async/37.0/job/"+jobID+"/batch", nil, query, jobResp, "text/csv")

	if nil != err {
		return nil, err
	}

	return jobResp, nil
}

func (forceApi *ForceApi) createInsertBatch(jobID string, in []SObject) (*CreateBatchResponse, error) {

	jobResp := &CreateBatchResponse{}
	err := forceApi.Post("/services/async/37.0/job/"+jobID+"/batch", nil, in, jobResp)

	if nil != err {
		return nil, err
	}

	return jobResp, nil
}

func (forceApi *ForceApi) getBatchStatus(jobID string, batchID string) (*CreateBatchResponse, error) {

	jobResp := &CreateBatchResponse{}
	err := forceApi.Get("/services/async/37.0/job/"+jobID+"/batch/"+batchID, nil, jobResp)

	if nil != err {
		return nil, err
	}

	return jobResp, nil
}

func (forceApi *ForceApi) getBatchResults(jobID string, batchID string) ([]*SObjectResponse, error) {

	jobResp := []*SObjectResponse{}
	err := forceApi.Get("/services/async/37.0/job/"+jobID+"/batch/"+batchID+"/result", nil, &jobResp)

	if nil != err {
		return nil, err
	}

	return jobResp, nil
}

func (forceApi *ForceApi) createJob(table string, operation string, contentType string) (*CreateJobResponse, error) {
	req := &CreateJobRequest{
		Operation:   operation,
		Object:      table,
		ContentType: contentType,
	}
	jobResp := &CreateJobResponse{}
	err := forceApi.Post("/services/async/37.0/job", nil, req, jobResp)

	if nil != err {
		return nil, err
	}

	return jobResp, nil
}

func (forceApi *ForceApi) closeJob(jobID string) (*CreateJobResponse, error) {
	req := &CloseJobRequest{
		State: "Closed",
	}

	jobResp := &CreateJobResponse{}
	err := forceApi.Post("/services/async/37.0/job/"+jobID, nil, req, jobResp)

	if nil != err {
		return nil, err
	}

	return jobResp, nil
}

func (forceApi *ForceApi) getJobStatus(jobID string) (*CreateJobResponse, error) {

	jobResp := &CreateJobResponse{}
	err := forceApi.Get("/services/async/37.0/job/"+jobID, nil, jobResp)

	if nil != err {
		return nil, err
	}

	return jobResp, nil
}

func (forceApi *ForceApi) InsertSObject(in SObject) (resp *SObjectResponse, err error) {
	if sObject, ok := forceApi.apiSObjects[in.ApiName()]; ok {
		uri := sObject.URLs[sObjectKey]

		resp = &SObjectResponse{}
		err = forceApi.Post(uri, nil, in.(interface{}), resp)
	} else {
		err = errors.New("Not found")
	}

	return
}

func (forceApi *ForceApi) UpdateSObject(id string, in SObject) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[in.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	err = forceApi.Patch(uri, nil, in.(interface{}), nil)

	return
}

func (forceApi *ForceApi) DeleteSObject(id string, in SObject) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[in.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	err = forceApi.Delete(uri, nil)

	return
}

func (forceApi *ForceApi) GetSObjectByExternalId(id string, fields []string, out SObject) (err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[out.ApiName()].URLs[sObjectKey],
		out.ExternalIdApiName(), id)

	params := url.Values{}
	if len(fields) > 0 {
		params.Add("fields", strings.Join(fields, ","))
	}

	err = forceApi.Get(uri, params, out.(interface{}))

	return
}

func (forceApi *ForceApi) UpsertSObjectByExternalId(id string, in SObject) (resp *SObjectResponse, err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[in.ApiName()].URLs[sObjectKey],
		in.ExternalIdApiName(), id)

	resp = &SObjectResponse{}
	err = forceApi.Patch(uri, nil, in.(interface{}), resp)

	return
}

func (forceApi *ForceApi) DeleteSObjectByExternalId(id string, in SObject) (err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[in.ApiName()].URLs[sObjectKey],
		in.ExternalIdApiName(), id)

	err = forceApi.Delete(uri, nil)

	return
}
