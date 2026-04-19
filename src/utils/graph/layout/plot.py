from collections.abc import Mapping, Sequence

import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.lines import Line2D

from utils.config import CLUBS_ORDERED, DEFAULT_CLUB_COLORS, DEFAULT_FALLBACK_COLOR
from utils.graph.layout.colors import _build_effective_club_colors
from utils.graph.layout.layout import _get_node_clubs, compute_club_layout


def _order_legend_clubs(
    seen_clubs: set[str],
    clubs_ordered: Sequence[str],
) -> list[str]:
    """Order legend labels by known clubs first and unknown clubs last.

    Args:
        seen_clubs: Clubs present in graph.
        clubs_ordered: Preferred club order.

    Returns:
        Ordered list of legend labels.
    """
    ordered_known = [club for club in clubs_ordered if club in seen_clubs]
    ordered_unknown = sorted(club for club in seen_clubs if club not in clubs_ordered)
    return ordered_known + ordered_unknown


def _build_legend_items(
    ordered_clubs: Sequence[str],
    club_colors: Mapping[str, str],
    fallback_color: str,
) -> list[Line2D]:
    """Create legend markers for club colors.

    Args:
        ordered_clubs: Clubs in legend order.
        club_colors: Mapping of club to color.
        fallback_color: Color for unknown clubs.

    Returns:
        Matplotlib legend handles.
    """
    items: list[Line2D] = []
    for club in ordered_clubs:
        color = club_colors.get(club, fallback_color)
        items.append(
            Line2D(
                [0],
                [0],
                marker="o",
                color="none",
                markerfacecolor=color,
                markeredgecolor=color,
                markersize=8,
                label=club,
            ),
        )
    return items


def _filter_graph_by_threshold(
    source_graph: nx.Graph,
    threshold_window: tuple[float, float],
) -> nx.Graph:
    """Create a graph with edges above threshold and preserve node attributes.

    Args:
        source_graph: Input graph.
        threshold_window: A tuple (lower, upper) representing the weight range to include.

    Returns:
        Thresholded graph with node attributes copied from source graph.
    """
    lower_threshold, upper_threshold = threshold_window
    filtered_graph = nx.Graph(
        (u, v, d)
        for u, v, d in source_graph.edges(data=True)
        if lower_threshold <= d.get("weight", 0.0) <= upper_threshold
    )

    for node in list(filtered_graph.nodes()):
        if node in source_graph.nodes:
            nx.set_node_attributes(filtered_graph, {node: source_graph.nodes[node]})

    return filtered_graph


def plot_graph_by_club(
    graph: nx.Graph,
    clubs_ordered: Sequence[str] = CLUBS_ORDERED,
    threshold_for_edges: tuple[float, float] | None = None,
    figsize: tuple[int, int] = (12, 8),
    node_size: int = 120,
    edge_alpha: float = 0.15,
    jitter: float = 0.1,
    seed: int = 42,
    title: str | None = None,
    fallback_label: str = "UNKNOWN",
    club_colors: Mapping[str, str] | None = None,
    fallback_color: str = DEFAULT_FALLBACK_COLOR,
    distinct_unknown_colors: bool = True,
    unknown_cmap: str = "tab20",
    show_node_ids: bool = False,
    node_id_font_size: int = 12,
) -> None:
    """Plot graph with club-based layout, colors, and legend.

    Args:
        graph: Input graph with node attribute "club".
        clubs_ordered: Ordered list of known clubs.
        threshold_for_edges: Optional minimum edge weight filter.
        figsize: Matplotlib figure size.
        node_size: Node marker size.
        edge_alpha: Edge opacity.
        jitter: Random angular jitter around each club axis.
        seed: Random seed for deterministic placement.
        title: Optional plot title.
        fallback_label: Label used when node has missing club.
        club_colors: Optional mapping from club to node color.
        fallback_color: Color used for unknown clubs.
        distinct_unknown_colors: Whether unknown clubs get distinct generated colors.
        unknown_cmap: Matplotlib colormap for generated unknown-club colors.
        show_node_ids: Whether to draw node ids on plot.
        node_id_font_size: Font size for node id labels.

    Returns:
        None
    """
    source_graph = graph
    if threshold_for_edges is not None:
        graph = _filter_graph_by_threshold(source_graph, threshold_for_edges)

    pos = compute_club_layout(
        graph,
        clubs_ordered=clubs_ordered,
        jitter=jitter,
        seed=seed,
        fallback_label=fallback_label,
    )

    if club_colors is None:
        club_colors = DEFAULT_CLUB_COLORS

    node_clubs = _get_node_clubs(graph, fallback_label)
    seen_clubs = set(node_clubs.values())
    effective_club_colors = _build_effective_club_colors(
        seen_clubs=seen_clubs,
        clubs_ordered=clubs_ordered,
        club_colors=club_colors,
        distinct_unknown_colors=distinct_unknown_colors,
        unknown_cmap=unknown_cmap,
    )
    node_colors = [
        effective_club_colors.get(node_clubs[node], fallback_color)
        for node in graph.nodes()
    ]

    plt.figure(figsize=figsize)
    nx.draw_networkx_nodes(
        graph,
        pos,
        node_size=node_size,
        alpha=0.9,
        node_color=node_colors,
    )
    nx.draw_networkx_edges(graph, pos, alpha=edge_alpha, width=0.6)

    if show_node_ids:
        labels = {node: str(node) for node in graph.nodes()}
        nx.draw_networkx_labels(graph, pos, labels=labels, font_size=node_id_font_size)

    ordered_legend_clubs = _order_legend_clubs(seen_clubs, clubs_ordered)
    legend_items = _build_legend_items(
        ordered_legend_clubs,
        effective_club_colors,
        fallback_color,
    )

    if legend_items:
        plt.legend(handles=legend_items, loc="best", frameon=True, title="Club")

    if title is None:
        title = f"Graph by clubs (nodes={graph.number_of_nodes()}, edges={graph.number_of_edges()})"
    plt.title(title)
    plt.axis("off")
    plt.tight_layout()
    plt.show()
