package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/providerbackbone"
)

// BackboneEntry maps a provider hostname to its upstream backbone. The backbone
// name is a suitable value for a provider's storage_group setting: providers
// sharing a backbone share article availability.
type BackboneEntry = providerbackbone.Entry

// handleProviderBackbones returns the host->backbone list used by the frontend to
// autofill a provider's storage_group. The result is cached in-memory for
// providerbackbone.TTL; on a fetch failure a previously cached copy is served.
// The cache is shared with the peer network, which reads it to recognise
// backbone labels.
//
//	@Summary		Get provider backbone map
//	@Description	Returns a cached mapping of usenet hostnames to their upstream backbone (storage group)
//	@Tags			providers
//	@Produce		json
//	@Success		200	{object}	map[string]interface{}
//	@Failure		503	{object}	map[string]interface{}
//	@Router			/providers/backbones [get]
func (s *Server) handleProviderBackbones(c *fiber.Ctx) error {
	entries, err := providerbackbone.Default.Get(c.Context(), nil)
	if err != nil {
		return RespondServiceUnavailable(c, "Failed to fetch provider backbone data", err.Error())
	}
	return RespondSuccess(c, entries)
}
