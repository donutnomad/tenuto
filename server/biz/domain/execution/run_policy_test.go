package execution_test

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ValidateRunPolicy 只接受两个策略取值；空串与未知值都是无效输入。
func TestValidateRunPolicy(t *testing.T) {
	for _, policy := range []string{execution.RunPolicyAutoUpgrade, execution.RunPolicyPinned} {
		if err := execution.ValidateRunPolicy(policy); err != nil {
			t.Fatalf("ValidateRunPolicy(%q): %v", policy, err)
		}
	}
	for _, policy := range []string{"", "manual", "auto"} {
		if err := execution.ValidateRunPolicy(policy); err == nil {
			t.Fatalf("ValidateRunPolicy(%q) accepted an invalid policy", policy)
		}
	}
}
