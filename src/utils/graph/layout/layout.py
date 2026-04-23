from collections.abc import Sequence

import networkx as nx
import numpy as np

from utils.config import CLUBS_ORDERED


def _normalize_club(raw_club: object, fallback_label: str) -> str:
    """Normalize raw club value to a safe display string.

    Args:
        raw_club: Raw club value from node attributes.
        fallback_label: Label used when club is missing.

    Returns:
        Normalized club label.
    """
    if raw_club is None or (isinstance(raw_club, float) and np.isnan(raw_club)):
        return fallback_label
    return str(raw_club)


def _build_known_angles(clubs_ordered: Sequence[str]) -> dict[str, float]:
    """Build angular mapping for ordered clubs.

    Args:
        clubs_ordered: Ordered list of clubs from left to right.

    Returns:
        Mapping from club to base angle.
    """
    known = list(clubs_ordered)
    if len(known) <= 1:
        return dict.fromkeys(known, np.pi)

    # Left-to-right ideological order: first club on the left, last on the right.
    return {club: np.pi - i * np.pi / (len(known) - 1) for i, club in enumerate(known)}


def _build_unknown_angles(unknown_clubs: Sequence[str]) -> dict[str, float]:
    """Build angular mapping for clubs outside the known order.

    Args:
        unknown_clubs: Unknown clubs sorted alphabetically.

    Returns:
        Mapping from unknown club to angle on lower semicircle.
    """
    if not unknown_clubs:
        return {}

    step = np.pi / (len(unknown_clubs) + 1)
    return {club: np.pi + idx * step for idx, club in enumerate(unknown_clubs, start=1)}


def _get_node_clubs(graph: nx.Graph, fallback_label: str) -> dict[object, str]:
    """Extract normalized club labels for graph nodes.

    Args:
        graph: Input graph.
        fallback_label: Label used for missing clubs.

    Returns:
        Mapping from node id to normalized club label.
    """
    return {
        node: _normalize_club(graph.nodes[node].get("club"), fallback_label)
        for node in graph.nodes()
    }


def compute_club_layout(
    graph: nx.Graph,
    clubs_ordered: Sequence[str] = CLUBS_ORDERED,
    jitter: float = 0.1,
    seed: int = 42,
    fallback_label: str = "UNKNOWN",
) -> dict:
    """Create 2D node positions based on political clubs with fallback for unknown clubs.

    Args:
        graph: Input graph.
        clubs_ordered: Ordered list of known clubs.
        jitter: Random angular jitter around each club axis.
        seed: Random seed for deterministic placement.
        fallback_label: Label used for missing club values.

    Returns:
        Mapping from node id to 2D position.
    """

    rng = np.random.default_rng(seed)
    known_angle = _build_known_angles(clubs_ordered)
    node_clubs = _get_node_clubs(graph, fallback_label)

    unknown_sorted = sorted(
        club for club in set(node_clubs.values()) if club not in known_angle
    )
    unknown_angle = _build_unknown_angles(unknown_sorted)

    pos = {}
    for node in graph.nodes():
        club = node_clubs[node]
        base_angle = known_angle.get(club, unknown_angle.get(club, 1.5 * np.pi))

        angle = base_angle + rng.uniform(-jitter, jitter)
        radius = rng.uniform(0.85, 1.15)

        pos[node] = (float(radius * np.cos(angle)), float(radius * np.sin(angle)))

    return pos
