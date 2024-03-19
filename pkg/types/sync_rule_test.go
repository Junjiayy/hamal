package types

import (
	"testing"
)

func TestSyncRule_evaluateFilterConditions(t *testing.T) {
	// ((a = 1 and (b = 2 and (c = 3 or d = 4))) and (e = 5 or f = 6)) or (g = 7 and (h = 8 or i = 9))
	conditions := map[string][]DataFilterCondition{
		ConditionTypeAnd: {
			{
				//(a = 1 and (b = 2 and (c = 3 or d = 4))) and (e = 5 or f = 6)
				Children: map[string][]DataFilterCondition{
					ConditionTypeAnd: {
						//(a = 1 and (b = 2 and (c = 3 or d = 4)))
						{
							Children: map[string][]DataFilterCondition{
								ConditionTypeAnd: {
									// a = 1
									{Column: "a", Operator: "=", Value: "1"},
									//(b = 2 and (c = 3 or d = 4))
									{
										Children: map[string][]DataFilterCondition{
											ConditionTypeAnd: {
												// b = 2
												{Column: "b", Operator: "=", Value: "2"},
												// (c = 3 or d = 4))
												{
													Children: map[string][]DataFilterCondition{
														ConditionTypeOr: {
															{Column: "c", Operator: "=", Value: "3"},
															{Column: "d", Operator: "!=", Value: "4"},
														},
													},
												},
											},
										},
									},
								},
							},
						},
						//(e = 5 or f = 6)
						{
							Children: map[string][]DataFilterCondition{
								ConditionTypeOr: {
									{Column: "e", Operator: "=", Value: "5"},
									{Column: "f", Operator: "!=", Value: "6"},
								},
							},
						},
					},
				},
			},
			{
				//g = 7 and (h = 8 or i = 9)
				Children: map[string][]DataFilterCondition{
					ConditionTypeAnd: {
						//g = 7
						{Column: "g", Operator: "=", Value: "7"},
						{
							// (h = 8 or i = 9)
							Children: map[string][]DataFilterCondition{
								ConditionTypeOr: {
									{Column: "h", Operator: "=", Value: "8"},
									{Column: "i", Operator: "!=", Value: "9"},
								},
							},
						},
					},
				},
			},
		},
	}

	rule := &SyncRule{}
	data := map[string]string{
		"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6",
		"g": "7", "h": "8", "i": "9",
	}

	ok := rule.evaluateFilterConditions(conditions, data)
	if !ok {
		t.Fatal("filter conditions failed")
	}
}
