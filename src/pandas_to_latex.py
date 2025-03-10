import os
from pathlib import Path
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

latex_code = r"""
\documentclass{article}
\usepackage{graphicx}  % for including graphics
\usepackage{float}     % for the [H] placement specifier
\usepackage{caption}   % for nicer captions
\begin{document}

\section*{Summary}
\subsection*{Successes}
\begin{itemize}
    \item Automated data pulls from WRDS OptionMetrics.
    \item Learned to use the WRDS API and web queries.
    \item Preprocessed data by accounting for trading days and correcting mis-stacked records.
    \item Improved code organization by making it modular rather than relying on one large function.
\end{itemize}

\subsection*{Challenges}
\begin{itemize}
    \item WRDS’s web query merges tables behind the scenes, making the process opaque (contacted Help Desk).
    \item Beyond checking nulls, duplicates, and plotting, it was hard to ensure full data cleanliness.
    \item Replicating paper results with index dividend yield introduced uncertainty about correctness.
    \item Dates were inconsistently formatted (e.g., \texttt{DatetimeIndex}, \texttt{Timestamp}, or \texttt{object} with timezones), requiring careful standardization.
\end{itemize}

\subsection*{Data Sources}
\begin{itemize}
    \item Bloomberg
    \item WRDS OptionMetrics
\end{itemize}

\newpage

\section*{Equity Index Spread Plots}
This section shows the Equity Index Spread plots.

\begin{figure}[H]
    \centering
    \includegraphics[width=0.8\textwidth]{equity_index_spread_plot_full_replication.pdf}
    \caption{Equity Index Spread - Full Replication}
    \label{fig:spread_full}
\end{figure}

\begin{figure}[H]
    \centering
    \includegraphics[width=0.8\textwidth]{equity_index_spread_plot_proxy.pdf}
    \caption{Equity Index Spread - Proxy Replication}
    \label{fig:spread_proxy}
\end{figure}

\newpage

\section*{Yearly Comparison of Time Series Data}
This figure compares the yearly evolution of the spread.

\begin{figure}[H]
    \centering
    \includegraphics[width=0.8\textwidth]{yearly_comparison.pdf}
    \caption{Yearly Comparison of Time Series Data}
    \label{fig:yearly}
\end{figure}

\newpage

\section*{Summary Statistics Tables}
This section shows the summary statistics tables.

\begin{figure}[H]
    \centering
    \includegraphics[width=0.8\textwidth]{table_full_replication.pdf}
    \caption{Summary statistics table - Full Replication}
    \label{fig:table_full}
\end{figure}

\begin{figure}[H]
    \centering
    \includegraphics[width=0.8\textwidth]{table_proxy_replication.pdf}
    \caption{Summary statistics table - Proxy Replication}
    \label{fig:table_proxy}
\end{figure}

\end{document}
"""

# Check if the output directory exists
if not OUTPUT_DIR.exists():
    os.makedirs(OUTPUT_DIR)

# Write the LaTeX code to a file in the output directory
with open(OUTPUT_DIR / "graph_document.tex", "w", encoding="utf-8") as f:
    f.write(latex_code)
