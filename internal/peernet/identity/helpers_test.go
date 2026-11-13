package identity

import (
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
)

func pemEncode(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}
func bigOne() *big.Int             { return big.NewInt(1) }
func pkixName(cn string) pkix.Name { return pkix.Name{CommonName: cn} }
