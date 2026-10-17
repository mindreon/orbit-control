package kernelapi

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
)

func TestTeamErrorCodesMatchContractEnum(t *testing.T) {
	want := map[string]struct{}{}
	for _, code := range []contract.TeamErrorCode{
		contract.TeamErrorCodeTEAMBADTRANSITION, contract.TeamErrorCodeTEAMFINISHED, contract.TeamErrorCodeTEAMMAILBOXFULL,
		contract.TeamErrorCodeTEAMNOTASSIGNEE, contract.TeamErrorCodeTEAMNOTINTURN, contract.TeamErrorCodeTEAMNOTLEADER,
		contract.TeamErrorCodeTEAMNOTREVIEWER, contract.TeamErrorCodeTEAMTEXTTOOLONG, contract.TeamErrorCodeTEAMTOOMANYTASKS,
		contract.TeamErrorCodeTEAMUNKNOWNMEMBER, contract.TeamErrorCodeTEAMUNKNOWNTASK, contract.TeamErrorCodeTEAMUPDATETIMEOUT,
	} {
		want[string(code)] = struct{}{}
	}
	require.Equal(t, want, teamErrorCodes)
}
