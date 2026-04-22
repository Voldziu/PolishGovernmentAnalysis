from collections.abc import Mapping, Sequence

import matplotlib.pyplot as plt
from matplotlib.colors import to_hex


def _build_unknown_club_colors(
    unknown_clubs: Sequence[str],
    unknown_cmap: str,
) -> dict[str, str]:
    """Generate deterministic colors for unknown clubs.

    Args:
        unknown_clubs: Unknown clubs sorted in stable order.
        unknown_cmap: Matplotlib colormap name used for unknown clubs.

    Returns:
        Mapping from unknown club to generated color.
    """
    if not unknown_clubs:
        return {}

    cmap = plt.get_cmap(unknown_cmap)
    bins = max(int(getattr(cmap, "N", 20)), 1)
    denominator = max(bins - 1, 1)

    # Step by 7 to spread neighboring clubs across palette bins.
    return {
        club: to_hex(cmap(((idx * 7) % bins) / denominator))
        for idx, club in enumerate(unknown_clubs)
    }


def _build_effective_club_colors(
    seen_clubs: set[str],
    clubs_ordered: Sequence[str],
    club_colors: Mapping[str, str],
    distinct_unknown_colors: bool,
    unknown_cmap: str,
) -> dict[str, str]:
    """Build final color mapping including optional colors for unknown clubs.

    Args:
        seen_clubs: Clubs present in graph.
        clubs_ordered: Preferred club order.
        club_colors: Base mapping for known clubs.
        distinct_unknown_colors: Whether to color unknown clubs distinctly.
        unknown_cmap: Matplotlib colormap used for unknown clubs.

    Returns:
        Final mapping used for node and legend colors.
    """
    effective_colors = dict(club_colors)
    if not distinct_unknown_colors:
        return effective_colors

    unknown_clubs = sorted(club for club in seen_clubs if club not in clubs_ordered)
    effective_colors.update(_build_unknown_club_colors(unknown_clubs, unknown_cmap))
    return effective_colors
