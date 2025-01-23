from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Image, Spacer
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
    elements.append(Spacer(1, 0.2 * inch))

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
    elements.append(Spacer(1, 0.3 * inch))

    # Define the second table's structure based on the image layout
    column_labels = ["SKU Code", "DB SKU", "SKU Description", "Proposed RSP (inc VAT)", 
                     "VAT", "Proposed RSP (ex VAT)", "RSP/C", "Retail Markup", 
                     "Retail Price LC", "W/Sale Markup", "BPTT LC/Case",
                     "Distributor Markup", "DPLC LC/Case", "Duty", "Clearing Charges", "CIF LC/Case"]

    table_data = []

    # Add header row
    table_data.append([
        Paragraph("Category", wrap_style),
        Paragraph("Value", wrap_style),
        Paragraph("%", wrap_style),
    ])

    # Add data rows
    for label, value, percentage in zip(column_labels, 
                                         [data["table_data"][0][0], data["table_data"][0][1], data["table_data"][0][2],
                                          None, data["table_data"][0][5], None, data["table_data"][0][6],data["table_data"][0][7] ,
                                          None, None,
                                          data["table_data"][0][8], data["table_data"][0][9], 
                                          data["table_data"][0][10], data["table_data"][0][11], 
                                          data["table_data"][0][16], None, data["table_data"][0][18]],  # Values
                                         [None, None, None, None, data["table_data"][0][4],
                                          None, None, None, None, None, None, None, 
                                          None, data["table_data"][0][17], data["table_data"][0][19]]  # Percentages
                                         ):
        table_data.append([
            Paragraph(label, wrap_style),
            Paragraph(str(value) if value is not None else "", wrap_style),
            Paragraph(str(percentage) if percentage is not None else "", wrap_style),
        ])

    # Create the table
    structured_table = Table(table_data, colWidths=[2 * inch, 3 * inch, 2 * inch])
    structured_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
    ]))

    elements.append(structured_table)

    # Build the PDF
    pdf.build(elements)
    return output_file
