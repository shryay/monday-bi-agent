"""Generate Decision Log PDF from markdown content."""

from fpdf import FPDF


class DecisionLogPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(120, 120, 120)
        self.cell(0, 8, "Decision Log - Monday.com BI Agent", align="R", new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(30, 30, 30)
        self.ln(3)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(50, 50, 50)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bullet(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(50, 50, 50)
        x = self.get_x()
        self.cell(6, 5.5, "-")
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def bold_body(self, bold_part, rest):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(50, 50, 50)
        self.write(5.5, bold_part)
        self.set_font("Helvetica", "", 10)
        self.write(5.5, rest)
        self.ln(7)


pdf = DecisionLogPDF()
pdf.alias_nb_pages()
pdf.set_auto_page_break(auto=True, margin=20)
pdf.add_page()

# Title
pdf.set_font("Helvetica", "B", 20)
pdf.set_text_color(20, 20, 20)
pdf.cell(0, 12, "Decision Log", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.set_font("Helvetica", "", 11)
pdf.set_text_color(100, 100, 100)
pdf.cell(0, 8, "Monday.com Business Intelligence Agent", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.ln(6)

# Section 1
pdf.section_title("1. Architecture & Tech Stack")

pdf.bold_body("Streamlit ", "was chosen as the UI framework because it provides a production-ready chat interface (st.chat_input, st.chat_message) with zero frontend code, built-in session state for conversation memory, and one-click deployment to Streamlit Cloud. For a 6-hour timeline, this eliminated the overhead of a separate frontend/backend setup while still delivering a polished, interactive prototype.")

pdf.bold_body("Groq (Llama 3.3 70B) ", "serves as the LLM engine via an OpenAI-compatible API. This choice was driven by three factors: (a) free tier with 14,400 requests/day, (b) strong tool-calling support that lets the AI autonomously decide which Monday.com boards to query, and (c) drop-in compatibility with OpenAI's Python SDK -- meaning the system also supports GPT-4o and Google Gemini with a single provider dropdown. The tool-calling approach (3 defined functions) is the core of the agent: the LLM interprets the user's question, selects the right tool with appropriate filters, and generates insights from the returned data -- all without hardcoded query logic.")

pdf.bold_body("Monday.com GraphQL API ", "with cursor-based pagination handles board data fetching. Each user query triggers fresh API calls (no caching), ensuring the evaluator always sees live data. The GraphQL schema allows fetching column definitions and item data in a single request, minimizing round trips.")

pdf.bold_body("pandas ", "handles all data cleaning -- null detection, date normalization, number parsing, and summary aggregation. A keyword-based column mapper tolerates naming differences between the original Excel headers and Monday.com's imported column titles.")

# Section 2
pdf.section_title("2. Handling Messy Data")

pdf.body_text("The provided datasets have significant quality challenges: 52% of deal values are missing, 75% of closure probabilities are null, the Work Orders sheet contains blank header rows, and date formats are inconsistent across columns. Rather than failing silently, the agent takes a transparent approach:")

pdf.bullet("Duplicate header rows (where 'Deal Status' appears as a data value) are detected and filtered during processing using exact-match exclusion against known header strings.")
pdf.bullet("Missing values are tracked per-column and reported as a completeness percentage in every response, so the founder knows the confidence level of the insight.")
pdf.bullet("Date parsing tries 8 format patterns (ISO, DD/MM/YYYY, US format, etc.) with graceful fallback to null. Monetary values are cleaned of commas, currency symbols, and whitespace before numeric conversion.")
pdf.bullet("Column name matching uses a keyword-based fuzzy mapper -- if Monday.com renames 'Masked Deal value' to 'Masked Deal Value' during import, it still maps correctly.")

# Section 3
pdf.section_title("3. Key Trade-offs")

pdf.bold_body("Summary-first vs raw-data approach. ", "Instead of sending raw board data to the LLM (expensive and token-limited), the agent pre-processes data into structured summaries -- status breakdowns, sector aggregations, pipeline metrics. This reduces token usage by ~90% and produces more focused insights. The trade-off is that the LLM cannot run ad-hoc calculations on raw rows, but for founder-level questions, pre-computed aggregates cover the vast majority of use cases.")

pdf.bold_body("Malformed tool-call recovery. ", "Llama 3.3 occasionally generates tool calls in an XML-like format (<function=name {...}>) instead of the expected JSON structure. Rather than failing, the agent catches these errors, parses the intended function and arguments from the error message using regex, executes the tool, and feeds the result back to the LLM. This makes the system significantly more robust in production without requiring a paid model.")

pdf.bold_body("Multi-provider support. ", "Supporting Groq, Gemini, and OpenAI through a unified interface adds slight complexity but demonstrates extensibility and allows the evaluator to test with their preferred provider.")

# Section 4
pdf.section_title("4. What I Would Improve With More Time")

pdf.bullet("Add chart/visualization generation (matplotlib or plotly) for trend data")
pdf.bullet("Implement semantic caching with TTL to reduce API calls for repeated questions")
pdf.bullet("Add Monday.com MCP server integration for a standardized tool protocol")
pdf.bullet("Build a feedback loop where the agent learns from corrected answers")
pdf.bullet("Add authentication so the hosted prototype doesn't require sidebar key entry")

output_path = "Decision_Log.pdf"
pdf.output(output_path)
print(f"PDF generated: {output_path}")
