package tenant

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
)

type OrgUrmService interface {
	FindResourceOrganizationID(ctx context.Context, rt influxdb.ResourceType, id influxdb.ID) (influxdb.ID, error)
}

type AuthedURMService struct {
	s          influxdb.UserResourceMappingService
	orgService OrgUrmService
}

func NewAuthedURMService(orgSvc OrgUrmService, s influxdb.UserResourceMappingService) *AuthedURMService {
	return &AuthedURMService{
		s:          s,
		orgService: orgSvc,
	}
}

func (s *AuthedURMService) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	urms, _, err := s.s.FindUserResourceMappings(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return authorizer.AuthorizeFindUserResourceMappings(ctx, s.orgService, urms)
}

func (s *AuthedURMService) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	orgID, err := s.orgService.FindResourceOrganizationID(ctx, m.ResourceType, m.ResourceID)
	if err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeWrite(ctx, m.ResourceType, m.ResourceID, orgID); err != nil {
		return err
	}
	return s.s.CreateUserResourceMapping(ctx, m)
}

func (s *AuthedURMService) DeleteUserResourceMapping(ctx context.Context, resourceID influxdb.ID, userID influxdb.ID) error {
	f := influxdb.UserResourceMappingFilter{ResourceID: resourceID, UserID: userID}
	urms, _, err := s.s.FindUserResourceMappings(ctx, f)
	if err != nil {
		return err
	}

	for _, urm := range urms {
		orgID, err := s.orgService.FindResourceOrganizationID(ctx, urm.ResourceType, urm.ResourceID)
		if err != nil {
			return err
		}
		if _, _, err := authorizer.AuthorizeWrite(ctx, urm.ResourceType, urm.ResourceID, orgID); err != nil {
			return err
		}
		if err := s.s.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil {
			return err
		}
	}
	return nil
}
