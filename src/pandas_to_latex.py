import os
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

latex_code = r"""
\documentclass{article}
\usepackage[margin=1in]{geometry}
\usepackage{graphicx}    % for includegraphics
\usepackage{float}       % for [H] specifier
\usepackage{caption}     % for nicer captions
\usepackage{adjustbox}   % for \resizebox

\begin{document}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Note and Summary Section (First Page)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\section*{Note and Summary}
\textbf{Note:}\\[0.5em]
\textbf{Full Data:} The full data figures and tables follow the paper's methodology exactly.\\
\textbf{Proxy Data:} The proxy data figures and tables use the index option embedded implied dividend yield.\\[1em]

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

\clearpage

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Full Data Figures (Vertical Arrangement)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\section*{Equity Index Spread Figures --- Full Data}
\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{equity_index_spread_plot_full_update.pdf}
    \caption{Equity Index Spread --- Full Update (Data until 2023-12-28)}
    \label{fig:full_update}
\end{figure}

\vspace{1em}

\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{equity_index_spread_plot_full_replication.pdf}
    \caption{Equity Index Spread --- Full Replication (Data until 2021-02-28)}
    \label{fig:full_replication}
\end{figure}

\clearpage

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Proxy Data Figures (Vertical Arrangement)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\section*{Equity Index Spread Figures --- Proxy Data}
\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{equity_index_spread_plot_proxy_update.pdf}
    \caption{Equity Index Spread --- Proxy Update (Data until 2024-08-31)}
    \label{fig:proxy_update}
\end{figure}

\vspace{1em}

\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{equity_index_spread_plot_proxy_replication.pdf}
    \caption{Equity Index Spread --- Proxy Replication (Data until 2021-02-28)}
    \label{fig:proxy_replication}
\end{figure}

\clearpage

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Yearly Comparison Figure
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\section*{Yearly Comparison of Time Series Data}
\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.6\textheight,keepaspectratio]{yearly_comparison.pdf}
    \caption{Yearly Comparison of Time Series Data. In general, the difference between the implied forward rate before and after maturity, as well as the difference among the forward rates embedded in different maturities, increases.}
    \label{fig:yearly}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Full Data Tables (Vertical Arrangement; Two Tables in One Figure)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\begin{figure}[!ht]
  \centering

  % 첫 번째 테이블
  \begin{minipage}{\textwidth}
    \centering
    \resizebox{0.9\textwidth}{!}{\includegraphics{table_full_update.pdf}}
    \caption{Summary Statistics Table --- Full Update (Data until 2023-12-28)}
    \label{tab:full_update}
  \end{minipage}
  
  \vspace{1em}  % 두 테이블 사이 간격
  
  % 두 번째 테이블
  \begin{minipage}{\textwidth}
    \centering
    \resizebox{0.9\textwidth}{!}{\includegraphics{table_full_replication.pdf}}
    \caption{Summary Statistics Table --- Full Replication (Data until 2021-02-28)}
    \label{tab:full_replication}
  \end{minipage}
\end{figure}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Proxy Data Tables (Vertical Arrangement; Two Tables in One Figure)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\begin{figure}[!ht]
  \centering

  % 첫 번째 테이블
  \begin{minipage}{\textwidth}
    \centering
    \resizebox{0.9\textwidth}{!}{\includegraphics{table_proxy_update.pdf}}
    \caption{Summary Statistics Table --- Proxy Update (Data until 2024-08-31)}
    \label{tab:proxy_update}
  \end{minipage}
  
  \vspace{1em}  % 두 테이블 사이 간격
  
  % 두 번째 테이블
  \begin{minipage}{\textwidth}
    \centering
    \resizebox{0.9\textwidth}{!}{\includegraphics{table_proxy_replication.pdf}}
    \caption{Summary Statistics Table --- Proxy Replication (Data until 2021-02-28)}
    \label{tab:proxy_replication}
  \end{minipage}

\end{figure}

\end{document}




"""

# Check if the output directory exists
if not OUTPUT_DIR.exists():
    os.makedirs(OUTPUT_DIR)

# Write the LaTeX code to a file in the output directory
with open(OUTPUT_DIR / "graph_document.tex", "w", encoding="utf-8") as f:
    f.write(latex_code)
