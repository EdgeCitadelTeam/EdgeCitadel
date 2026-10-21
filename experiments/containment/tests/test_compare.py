"""The pre-registered one-sided tests of H10 (b)."""

from __future__ import annotations

import math

from harness.compare import fisher_one_sided, mcnemar_one_sided


def test_mcnemar_one_sided_is_the_binomial_upper_tail():
    assert mcnemar_one_sided(5, 0) == 1 / 32
    assert mcnemar_one_sided(3, 1) == 5 / 16
    assert mcnemar_one_sided(0, 4) == 1.0
    assert mcnemar_one_sided(0, 0) == 1.0


def test_fisher_one_sided_is_the_hypergeometric_upper_tail():
    # Firebreak 10 of 10 clean against 3 of 10: only the observed table is as
    # extreme, C(10,10) C(10,3) / C(20,13).
    assert math.isclose(fisher_one_sided(10, 10, 3, 10), 120 / math.comb(20, 13))
    assert math.isclose(
        fisher_one_sided(5, 10, 5, 10),
        sum(math.comb(10, k) * math.comb(10, 10 - k) for k in range(5, 11))
        / math.comb(20, 10),
    )
