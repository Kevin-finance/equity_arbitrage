import os
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

latex_code = r"""
\documentclass{article}

% Language setting
% Replace `english' with e.g. `spanish' to change the document language
\usepackage[english]{babel}

% Set page size and margins
% Replace `letterpaper' with`a4paper' for UK/EU standard size
\usepackage[letterpaper,top=2cm,bottom=2cm,left=3cm,right=3cm,marginparwidth=1.75cm]{geometry}

% Useful packages
\usepackage{float}
\usepackage{amsmath}
\usepackage{graphicx}
\usepackage[colorlinks=true, allcolors=blue]{hyperref}

\title{Replicating Equity Spot-Futures Arbitrage Spreads: A Reproducible Analytical Pipeline Approach}
\author{Young Jae Jung, Mooseok Kang}

\begin{document}
\maketitle


\section{Introduction}
The study of arbitrage spreads provides crucial insights into market inefficiencies and asset pricing anomalies. This project aims to replicate the Equity Spot-Futures Arbitrage Spreads examined in the paper Segmented Arbitrage by Emil Siriwardane, Adi Sunderam, and Jonathan Wallen. Specifically, the focus is on Panel 11: Equity Spot-Futures Arbitrage from Figure A1 in the Appendix. \par

The objective of this project is to reproduce the arbitrage-implied forward rates for the S\&P 500, Dow Jones, and Nasdaq 100 futures contracts, as documented in the original study. The key contributions of this project include:

\begin{itemize}
    \item Automating the end-to-end data retrieval, processing, and visualization of arbitrage spreads.
    \item Implementing a Reproducible Analytical Pipeline (RAP) to ensure integrity and scalability of the analysis.
    \item Converting {Stata-based methodologies} from the original study into a \textbf{Python-based framework}.
    \item Using OptionMetrics (WRDS) implied dividend yields as a \textbf{proxy dataset} for cases where Bloomberg data is unavailable.
\end{itemize}

\section{Background and Literature Review}
Arbitrage refers to the simultaneous buying and selling of an asset to profit from price discrepancies. While traditional asset pricing models assume that arbitrage opportunities should not exist due to market efficiency, the paper \textit{Segmented Arbitrage} challenges this assumption by documenting persistent arbitrage spreads in various financial markets.

The \textbf{equity spot-futures arbitrage} trade exploits differences between:
\begin{itemize}
    \item \textbf{Spot prices} of equity indices (S\&P 500, Dow Jones, Nasdaq 100)
    \item \textbf{Futures prices} with different maturities
    \item \textbf{Implied forward rates} calculated from these relationships
\end{itemize}

In an ideal arbitrage-free market, the implied forward rate should be predictable using interest rates and dividend yields. However, empirical observations suggest that \textbf{institutional constraints and market segmentation} can lead to persistent deviations from theoretical no-arbitrage conditions.

The original study relies on Bloomberg data to construct these arbitrage spreads. However, in this project, \textbf{OptionMetrics index option-implied dividend yields} serve as an alternative proxy dataset.
\section{Data Sources and Collection}
\subsection{Full Data (Original Study)}
\begin{itemize}
    \item \textbf{Source:} Bloomberg
    \item \textbf{Usage:} The original paper constructs arbitrage-implied forward rates using futures contracts data from Bloomberg.
    \item \textbf{Challenges:} Bloomberg is a proprietary data source, limiting access to certain users.
\end{itemize}

\subsection{Proxy Data (Alternative)}
\begin{itemize}
    \item \textbf{Source:} WRDS \textbf{OptionMetrics}
    \item \textbf{Usage:} Uses \textbf{index option-embedded implied dividend yields} to approximate expected dividends.
    \item \textbf{Advantages:} This approach enables the replication of key results without requiring proprietary Bloomberg access.
    \item \textbf{Challenges:} Implied dividend yields derived from options may not perfectly replicate the original dataset.
\end{itemize}

\section{Methodology}
\subsection{Calculation of Arbitrage-Implied Forward Rates}

The focus is on equity spot-futures arbitrage spreads using data from the S\&P 500 (SPX), Nasdaq 100 (NDX), and Dow Jones Industrial Average (DJI).

We follow the methodology outlined in the paper to compute arbitrage-implied forward rates:
\[
 1 + f_{\tau1,\tau2,t} = \frac{F_{t,\tau2} + E^Q_t[D_{t,\tau2}]}{F_{t,\tau1} + E^Q_t[D_{t,\tau1}]} 
\]

The arbitrage spread is computed as:
\[
 ESF_t = f_{\tau1,\tau2,t} - OIS3M_t 
\]


\subsection{Data Processing Pipeline}
The project follows a structured pipeline to ensure reproducibility:
\begin{enumerate}
    \item \textbf{Data Retrieval}: Automate Bloomberg and WRDS OptionMetrics queries.
    \item \textbf{Preprocessing}: Standardize date formats, correct trading day inconsistencies, and filter missing values.
    \item \textbf{Spread Calculation}: Compute implied forward rates using the formula above.
    \item \textbf{Visualization}: Generate time series plots to compare results with the original study.
\end{enumerate}
\clearpage
\section{Results and Findings}
\subsection{Equity Index Spread Figures --- Original Paper}
\begin{figure}[!ht]
      \centering
      \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{../data_manual/plot_research_paper.png}
      \caption{Equity Index Spread --- Appendix from the Original Paper}
      \label{fig:full_update}
  \end{figure}
\subsection{Equity Index Spread Figures --- Full Data}
\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{../_output/equity_index_spread_plot_full_update.pdf}
    \caption{Equity Index Spread --- Full Update (Data until 2023-12-28)}
    \label{fig:full_update}
\end{figure}

\vspace{1em}

\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{../_output/equity_index_spread_plot_full_replication.pdf}
    \caption{Equity Index Spread --- Full Replication (Data until 2021-02-28)}
    \label{fig:full_replication}
\end{figure}

\clearpage
\subsection{Equity Index Spread Figures --- Proxy Data}
\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{../_output/equity_index_spread_plot_proxy_update.pdf}
    \caption{Equity Index Spread --- Proxy Update (Data until 2024-08-31)}
    \label{fig:proxy_update}
\end{figure}

\vspace{1em}

\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.38\textheight,keepaspectratio]{../_output/equity_index_spread_plot_proxy_replication.pdf}
    \caption{Equity Index Spread --- Proxy Replication (Data until 2021-02-28)}
    \label{fig:proxy_replication}
\end{figure}

\clearpage
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Yearly Comparison Figure
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\subsection{Yearly Comparison of Time Series Data}
\begin{figure}[!ht]
    \centering
    \includegraphics[width=\textwidth,height=0.6\textheight,keepaspectratio]{../_output/yearly_comparison.pdf}
    \caption{Yearly Comparison of Time Series Data. In general, the difference between the implied forward rate before and after maturity, as well as the difference among the forward rates embedded in different maturities, increases.}
    \label{fig:yearly}
\end{figure}
\clearpage
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Full Data Tables (Vertical Arrangement; Two Tables in One Figure)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\subsection{Summary Statistics Tables --- Full Data} 
\begin{figure}[!ht]
  \centering

  % 첫 번째 테이블
  \begin{minipage}{\textwidth}
    \centering
    \resizebox{0.85\textwidth}{!}{\includegraphics{../_output/table_full_update.pdf}}
    \caption{Summary Statistics Table --- Full Update (Data until 2023-12-28)}
    \label{tab:full_up to date}
  \end{minipage}

  % 두 번째 테이블
  \begin{minipage}{\textwidth}
    \centering
    \resizebox{0.85\textwidth}{!}{\includegraphics{../_output/table_full_replication.pdf}}
    \caption{Summary Statistics Table --- Full Replication (Data until 2021-02-28)}
    \label{tab:full_replication}
  \end{minipage}
\end{figure}
\clearpage
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Proxy Data Tables (Vertical Arrangement; Two Tables in One Figure)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\subsection{Summary Statistics Tables --- Proxy Data}
\begin{figure}[!ht]
  \centering

  % 첫 번째 테이블
  \begin{minipage}{\textwidth}
    \centering
    \resizebox{0.85\textwidth}{!}{\includegraphics{../_output/table_proxy_update.pdf}}
    \caption{Summary Statistics Table --- Proxy Update (Data until 2024-08-31)}
    \label{tab:proxy_update}
  \end{minipage}
  
  % 두 번째 테이블
  \begin{minipage}{\textwidth}
    \centering
    \resizebox{0.85\textwidth}{!}{\includegraphics{../_output/table_proxy_replication.pdf}}
    \caption{Summary Statistics Table --- Proxy Replication (Data until 2021-02-28)}
    \label{tab:proxy_replication}
  \end{minipage}

\end{figure}

\section{Challenges and Limitations}
\begin{itemize}
    \item \textbf{Data Access and Cleaning}: Bloomberg data is proprietary, requiring alternative \textbf{proxy estimates using OptionMetrics}.
    \item \textbf{Methodological Challenges}: Matching results exactly was difficult due to minor methodological differences in data sources.
    \item \textbf{WRDS OptionMetrics Table Issue}: We encountered an issue with the WRDS OptionMetrics table where the results obtained from a web query differed from those extracted using SQL. We contacted the help desk via \href{https://wrds-support.wharton.upenn.edu/hc/en-us/requests/new?ticket_form_id=114093978532}{this link} and received SAS code, which we later converted into Python SQL.
    \item \textbf{Data Processing Challenges}: In data processing, challenges arose due to null values and duplicate entries, and although we generated plots, we were not completely sure if any unexpected data errors remained. For example, before 2017, the expiration date for index options was set as the day following the actual expiration.
    \item \textbf{Collaboration Issues}: Collaboration was further complicated by differences in code behavior between MacOS and Windows. Additionally, GitHub Actions runs on MacOS, which led to build failures because some libraries only operate on Windows.
\end{itemize}


\section{Conclusion}
This project successfully replicated \textbf{Equity Spot-Futures Arbitrage Spreads}, demonstrating that:
\begin{itemize}
    \item The original paper’s findings hold under updated datasets.
    \item \textbf{OptionMetrics implied dividend yields} serve as a viable alternative to Bloomberg.
    \item A \textbf{Reproducible Analytical Pipeline (RAP)} ensures long-term scalability.
\end{itemize}

\begin{thebibliography}{9}
\bibitem{segmented_arbitrage}
Emil Siriwardane, Adi Sunderam, and Jonathan Wallen, \textit{Segmented Arbitrage}, Harvard Business School Working Paper, 2023.
\end{thebibliography}
\end{document}


"""

# Check if the output directory exists
if not OUTPUT_DIR.exists():
    os.makedirs(OUTPUT_DIR)

# Write the LaTeX code to a file in the output directory
with open(OUTPUT_DIR / "graph_document.tex", "w", encoding="utf-8") as f:
    f.write(latex_code)
