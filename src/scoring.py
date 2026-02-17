# src/scoring.py

from typing import Optional


def compute_score(
    *,
    mobility_fit: Optional[int],
    security_fit: Optional[int],
    voip_fit: Optional[int],
    fleet_attach: Optional[int],
    rating: Optional[float],
    review_count: Optional[int],
    has_website: bool,
    has_opening_hours: bool,
) -> float:
    """
    Mobility-first weighted score (0–100 capped)
    """

    mobility = mobility_fit or 0
    security = security_fit or 0
    voip = voip_fit or 0
    fleet = fleet_attach or 0

    score = (
        0.55 * mobility +
        0.20 * security +
        0.15 * voip +
        0.10 * fleet
    )

    # Boost signals
    if rating and rating >= 4.2:
        score += 5

    if review_count and review_count >= 10:
        score += 5

    if has_website:
        score += 5

    if has_opening_hours:
        score += 5

    return min(score, 100.0)