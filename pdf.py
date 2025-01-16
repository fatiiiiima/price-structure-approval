from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from tempfile import NamedTemporaryFile

def generate_price_structure_pdf(data):
    # Create a temporary PDF file
    with NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
        output_file = temp_file.name

    pdf = SimpleDocTemplate(output_file, pagesize=landscape(letter))
    elements = []

    # Styles
    styles = getSampleStyleSheet()
    title_style = styles['Title']
    normal_style = styles['Normal']
    wrap_style = styles['Normal']
    wrap_style.fontSize = 8  # Adjust font size for readability
    wrap_style.leading = 10  # Line spacing for wrapped text

    # Add title
    elements.append(Paragraph("Price Communication to Distributor – Price Structure", title_style))

    # Add basic details
    basic_details = [
        ["Date of Communication", data["date_of_communication"]],
        ["Distributor", data["distributor"]],
        ["Country", data["country"]],
        ["Effective date from", data["effective_date_from"]],
        ["Effective date till", data["effective_date_till"]],
        ["Invoicing Currency", data["invoicing_currency"]],
        ["Lipton Invoicing Entity", data["invoicing_entity"]],
        ["Lipton Finance Member", data["finance_member"]],
    ]

    details_table = Table(basic_details, colWidths=[2 * inch, 4 * inch])
    details_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
    ]))
    elements.append(details_table)
    elements.append(Paragraph("<br/><br/>", normal_style))

    # Add table data (detailed price structure)
    column_titles = [
        "Entity", "SKU Code", "DB SKU", "SKU Description", "Proposed RSP (inc VAT)", 
        "VAT %", "VAT", "Proposed RSP (ex VAT)", "RSP/C", "RM%", "Retail Markup LC", 
        "Retail Price LC", "WSM %", "W/Sale Markup LC", "BPTT LC/Case", "DM%", 
        "Distributor Markup LC", "DPLC LC/Case", "Duty %", "Duty", "Clearing Charges %", 
        "Clearing Charges", "CIF LC/Case"
    ]

    wrapped_column_titles = [Paragraph(title, wrap_style) for title in column_titles]

    # Prepare detailed data
    detailed_data = [wrapped_column_titles]

    # Wrap text in table_data
    for row in data["table_data"]:
        wrapped_row = [Paragraph(str(cell), wrap_style) for cell in row]
        detailed_data.append(wrapped_row)

    # Define dynamic column widths
    column_widths = [
        0.4 * inch,  # Entity
        0.4 * inch,  # SKU Code
        0.4 * inch,  # DB SKU
        1.4 * inch,  # SKU Description (wider)
        0.4 * inch,  # Proposed RSP (inc VAT)
        0.4 * inch,  # VAT %
        0.4 * inch,  # VAT
        0.4 * inch,  # Proposed RSP (ex VAT)
        0.4 * inch,  # RSP/C
        0.4 * inch,  # RM%
        0.4 * inch,  # Retail Markup LC
        0.4 * inch,  # Retail Price LC
        0.4 * inch,  # WSM %
        0.4 * inch,  # W/Sale Markup LC
        0.4 * inch,  # BPTT LC/Case
        0.4 * inch,  # DM%
        0.4 * inch,  # Distributor Markup LC
        0.4 * inch,  # DPLC LC/Case
        0.4 * inch,  # Duty %
        0.4 * inch,  # Duty
        0.4 * inch,  # Clearing Charges %
        0.4 * inch,  # Clearing Charges
        0.4 * inch   # CIF LC/Case
    ]

    detailed_table = Table(detailed_data, colWidths=column_widths)
    detailed_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
    ]))
    elements.append(detailed_table)

    # Add logo
    logo_path = "lipton_logo.png"  # Replace with the actual logo path
    elements.insert(0, Image(logo_path, width=2 * inch, height=1 * inch))

    # Build the PDF
    pdf.build(elements)
    return output_file
