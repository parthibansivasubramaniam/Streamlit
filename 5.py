import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import base64
import io
import openai
from openai import OpenAI
from openai import AzureOpenAI
import json
import re
from typing import Dict, List, Tuple, Optional, Any
import numpy as np

# Initialize ALL session state variables at the beginning
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = {}

if 'openai_client' not in st.session_state:
    st.session_state.openai_client = None

if 'model_name' not in st.session_state:
    st.session_state.model_name = "gpt-4o"

if 'reconciliation_results' not in st.session_state:
    st.session_state.reconciliation_results = {}

if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

if 'chat_input' not in st.session_state:
    st.session_state.chat_input = ""

if 'azure_client' not in st.session_state:
    st.session_state.azure_client = None

def extract_text_from_pdf(file_content: bytes) -> str:
    """Extract text content from PDF for analysis"""
    try:
        # This is a placeholder for text extraction
        # In a real implementation, you'd use PyPDF2, pdfplumber, or similar
        # For now, we'll return a status message
        return "PDF text extraction requires additional libraries (PyPDF2/pdfplumber)"
    except Exception as e:
        return f"Text extraction failed: {str(e)}"

def analyze_pdf_structure(file_content: bytes, filename: str) -> Dict[str, Any]:
    """Analyze PDF structure for reconciliation purposes"""
    try:
        # Basic PDF analysis
        pdf_analysis = {
            'file_size': len(file_content),
            'filename': filename,
            'estimated_pages': len(file_content) // 50000,  # Rough estimate
            'content_type': 'financial_document',  # Default assumption
            'structure_indicators': []
        }
        
        # Try to identify document patterns
        if b'invoice' in file_content.lower() or b'bill' in file_content.lower():
            pdf_analysis['content_type'] = 'invoice'
            pdf_analysis['structure_indicators'].append('invoice_format')
        
        if b'statement' in file_content.lower() or b'balance' in file_content.lower():
            pdf_analysis['content_type'] = 'statement'
            pdf_analysis['structure_indicators'].append('statement_format')
        
        if b'report' in file_content.lower():
            pdf_analysis['content_type'] = 'report'
            pdf_analysis['structure_indicators'].append('report_format')
        
        # Look for table indicators
        table_indicators = [b'table', b'row', b'column', b'total', b'amount', b'date']
        table_score = sum(1 for indicator in table_indicators if indicator in file_content.lower())
        pdf_analysis['table_likelihood'] = min(table_score / len(table_indicators), 1.0)
        
        return pdf_analysis
        
    except Exception as e:
        return {'error': f"PDF structure analysis failed: {str(e)}"}

def enhanced_pdf_processing(file) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """Enhanced PDF processing with better structure analysis"""
    try:
        file_content = file.getvalue()
        base64_pdf = base64.b64encode(file_content).decode('utf-8')
        pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="700" height="1000" type="application/pdf"></iframe>'
        
        # Analyze PDF structure
        structure_analysis = analyze_pdf_structure(file_content, file.name)
        
        # Enhanced PDF processing result
        pdf_result = {
            'type': 'pdf',
            'content': pdf_display,
            'has_tables': False,
            'structure_analysis': structure_analysis,
            'reconciliation_ready': False,
            'content_summary': {}
        }
        
        # Try table extraction
        df = None
        try:
            file.seek(0)
            tables = pd.read_pdf(io.BytesIO(file_content), pages='all', multiple_tables=True)
            
            if tables and len(tables) > 0:
                combined_tables = []
                for i, table in enumerate(tables):
                    if not table.empty:
                        table['PDF_Table_Number'] = i + 1
                        table['Source_PDF'] = file.name
                        combined_tables.append(table)
                
                if combined_tables:
                    df = pd.concat(combined_tables, ignore_index=True)
                    df = df.dropna(how='all').dropna(axis=1, how='all')
                    
                    if not df.empty:
                        pdf_result['has_tables'] = True
                        pdf_result['reconciliation_ready'] = True
                        pdf_result['content_summary'] = {
                            'tables_found': len(tables),
                            'total_rows': len(df),
                            'columns': list(df.columns),
                            'data_types': df.dtypes.astype(str).to_dict()
                        }
        
        except Exception as table_error:
            pdf_result['table_extraction_error'] = str(table_error)
        
        # Extract text for content analysis
        try:
            text_content = extract_text_from_pdf(file_content)
            pdf_result['text_preview'] = text_content[:500] if len(text_content) > 500 else text_content
        except Exception as text_error:
            pdf_result['text_extraction_error'] = str(text_error)
        
        return df, pdf_result
        
    except Exception as e:
        return None, {'type': 'pdf', 'error': f"Enhanced PDF processing failed: {str(e)}"}

def read_file(file):
    """Enhanced file reading with better PDF support"""
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
            return df, None
        elif file.name.endswith('.xlsx'):
            xl = pd.ExcelFile(file)
            if len(xl.sheet_names) > 1:
                df = pd.concat([pd.read_excel(file, sheet_name=sheet).assign(Sheet=sheet)
                               for sheet in xl.sheet_names], ignore_index=True)
            else:
                df = pd.read_excel(file, sheet_name=0)
            return df, None
        elif file.name.endswith('.pdf'):
            return enhanced_pdf_processing(file)
        else:
            return None, "Unsupported file type."
    except Exception as e:
        return None, f"Error processing {file.name}: {str(e)}"

def display_csv_analysis(df, filename):
    """Display comprehensive CSV analysis"""
    st.markdown(f"#### 📊 CSV Analysis for {filename}")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Rows", len(df))
    with col2:
        st.metric("Columns", len(df.columns))
    with col3:
        st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
    with col4:
        missing_values = df.isnull().sum().sum()
        st.metric("Missing Values", missing_values)
    
    st.markdown("##### 📋 Data Preview")
    st.dataframe(df, use_container_width=True, height=400)
    
    st.markdown("##### 📝 Column Information")
    col_info = pd.DataFrame({
        'Column': df.columns,
        'Data Type': df.dtypes,
        'Non-Null Count': df.count(),
        'Null Count': df.isnull().sum(),
        'Unique Values': df.nunique()
    })
    st.dataframe(col_info, use_container_width=True)
    
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        st.markdown("##### 📈 Numeric Summary Statistics")
        st.dataframe(df[numeric_cols].describe(), use_container_width=True)

def display_enhanced_pdf_analysis(pdf_info, filename):
    """Display enhanced PDF analysis"""
    st.markdown(f"#### 📄 PDF Analysis for {filename}")
    
    structure = pdf_info.get('structure_analysis', {})
    content_summary = pdf_info.get('content_summary', {})
    
    # Basic metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("File Size", f"{structure.get('file_size', 0) / 1024:.1f} KB")
    with col2:
        st.metric("Est. Pages", structure.get('estimated_pages', 'Unknown'))
    with col3:
        st.metric("Content Type", structure.get('content_type', 'Unknown').title())
    with col4:
        table_ready = "✅ Yes" if pdf_info.get('reconciliation_ready', False) else "❌ No"
        st.metric("Recon Ready", table_ready)
    
    # Structure analysis
    if structure.get('structure_indicators'):
        st.markdown("##### 🔍 Document Structure")
        indicators = ", ".join(structure['structure_indicators'])
        st.info(f"Detected patterns: {indicators}")
    
    # Table analysis if available
    if pdf_info.get('has_tables') and content_summary:
        st.markdown("##### 📊 Extracted Table Data")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Tables Found", content_summary.get('tables_found', 0))
            st.metric("Total Rows", content_summary.get('total_rows', 0))
        with col2:
            st.metric("Columns", len(content_summary.get('columns', [])))
            table_likelihood = structure.get('table_likelihood', 0)
            st.metric("Table Quality", f"{table_likelihood:.1%}")
        
        if content_summary.get('columns'):
            st.markdown("**Columns Found:**")
            st.write(", ".join(content_summary['columns']))
    
    # Content preview
    if pdf_info.get('text_preview'):
        with st.expander("📝 Text Preview", expanded=False):
            st.text(pdf_info['text_preview'])
    
    # Display the PDF
    st.markdown("##### 📋 PDF Document")
    st.markdown(pdf_info['content'], unsafe_allow_html=True)

def setup_openai_client():
    """Setup Azure OpenAI client with user credentials"""
    st.sidebar.markdown("### 🔐 Azure OpenAI Configuration")
    
    azure_endpoint = st.sidebar.text_input(
        "Azure OpenAI Endpoint",
        placeholder="https://the-hack-backpapers-1.openai.azure.com/",
        help="Your Azure OpenAI resource endpoint"
    )
    
    api_key = st.sidebar.text_input(
        "API Key",
        type="password",
        help="Your Azure OpenAI API key"
    )
    
    api_version = st.sidebar.selectbox(
        "API Version",
        ["2024-02-15-preview", "2023-12-01-preview", "2023-10-01-preview"],
        index=0,
        help="Azure OpenAI API version"
    )
    
    deployment_name = st.sidebar.text_input(
        "Deployment Name",
        placeholder="gpt-4o",
        help="Your GPT model deployment name"
    )
    
    if st.sidebar.button("🔗 Connect to Azure OpenAI"):
        if azure_endpoint and api_key and deployment_name:
            try:
                client = AzureOpenAI(
                    azure_endpoint=azure_endpoint,
                    api_key=api_key,
                    api_version=api_version
                )
                st.session_state.azure_client = client
                st.session_state.deployment_name = deployment_name
                st.sidebar.success("✅ Connected to Azure OpenAI!")
            except Exception as e:
                st.sidebar.error(f"❌ Connection failed: {str(e)}")
        else:
            st.sidebar.error("❌ Please fill in all required fields")
    
    if st.session_state.azure_client:
        st.sidebar.success("🟢 Azure OpenAI Connected")
        return True
    else:
        st.sidebar.warning("🟡 Azure OpenAI Not Connected")
        return False


def prepare_data_for_analysis(files_data):
    """Enhanced data preparation with better PDF support"""
    analysis_data = {}
    
    for filename, file_info in files_data.items():
        df = file_info['df']
        text_content = file_info['text_content']
        
        # Handle tabular data files
        if df is not None and not df.empty:
            file_analysis = {
                'file_type': 'tabular_data',
                'source_format': filename.split('.')[-1].upper(),
                'total_rows': len(df),
                'columns': list(df.columns),
                'data_types': df.dtypes.astype(str).to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'missing_percentage': (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
                'unique_counts': df.nunique().to_dict(),
                'sample_data': df.head(15).to_dict('records'),
                'column_stats': {}
            }
            
            # Add column statistics
            for col in df.columns:
                col_stats = {
                    'data_type': str(df[col].dtype),
                    'non_null_count': df[col].count(),
                    'unique_values': df[col].nunique()
                }
                
                if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                    col_stats.update({
                        'min': df[col].min() if pd.notna(df[col].min()) else None,
                        'max': df[col].max() if pd.notna(df[col].max()) else None,
                        'mean': df[col].mean() if pd.notna(df[col].mean()) else None,
                        'std': df[col].std() if pd.notna(df[col].std()) else None
                    })
                elif df[col].dtype == 'object':
                    try:
                        col_stats.update({
                            'sample_values': df[col].dropna().head(5).tolist(),
                            'avg_length': df[col].astype(str).str.len().mean() if not df[col].empty else 0
                        })
                    except:
                        pass
                
                file_analysis['column_stats'][col] = col_stats
            
            # Handle PDF-specific information
            if isinstance(text_content, dict) and text_content.get('type') == 'pdf':
                file_analysis['pdf_metadata'] = {
                    'structure_analysis': text_content.get('structure_analysis', {}),
                    'content_summary': text_content.get('content_summary', {}),
                    'reconciliation_ready': text_content.get('reconciliation_ready', False)
                }
                file_analysis['note'] = 'Data extracted from PDF with enhanced analysis'
            
            analysis_data[filename] = file_analysis
        
        # Handle PDF files without extractable data
        elif isinstance(text_content, dict) and text_content.get('type') == 'pdf':
            pdf_analysis = {
                'file_type': 'pdf_document',
                'source_format': 'PDF',
                'structure_analysis': text_content.get('structure_analysis', {}),
                'has_extractable_data': False,
                'reconciliation_strategy': 'content_comparison'
            }
            
            if text_content.get('text_preview'):
                pdf_analysis['text_preview'] = text_content['text_preview']
                pdf_analysis['reconciliation_strategy'] = 'text_based_comparison'
            
            if not text_content.get('reconciliation_ready'):
                pdf_analysis['recommendation'] = 'Enhanced OCR or manual extraction may be required'
            
            analysis_data[filename] = pdf_analysis
    
    return analysis_data

def generate_pdf_reconciliation_prompt(analysis_data: Dict[str, Any]) -> str:
    """Generate specialized prompt for PDF-to-PDF reconciliation"""
    pdf_files = [f for f, data in analysis_data.items() if 'pdf' in data.get('file_type', '').lower()]
    
    # Count different types of PDF files
    extractable_pdfs = [f for f in pdf_files if analysis_data[f].get('has_extractable_data', False)]
    text_based_pdfs = [f for f in pdf_files if 'text_preview' in analysis_data[f]]
    
    prompt_sections = [
        f"You are a senior financial reconciliation expert specializing in PDF document analysis and cross-format reconciliation. I have uploaded {len(analysis_data)} files including {len(pdf_files)} PDF files for comprehensive reconciliation analysis.",
        "",
        "FILE COMPOSITION:",
        f"- Total files: {len(analysis_data)}",
        f"- PDF files: {len(pdf_files)}",
        f"- PDFs with extractable data: {len(extractable_pdfs)}",
        f"- PDFs with text content: {len(text_based_pdfs)}",
        "",
        "DETAILED FILE ANALYSIS:",
        json.dumps(analysis_data, indent=2, default=str),
        "",
        "PROVIDE A COMPREHENSIVE PDF-FOCUSED RECONCILIATION ANALYSIS with these sections:",
        "",
        "## 🔍 **EXECUTIVE SUMMARY**",
        "- Overall PDF reconciliation feasibility assessment",
        "- Key findings from PDF structure analysis",
        "- Risk assessment for PDF-based reconciliation",
        "- Success probability estimates",
        "",
        "## 📄 **PDF DOCUMENT ANALYSIS**",
        "- Document type classification and compatibility",
        "- Structure analysis results for each PDF",
        "- Data extraction success rates and quality",
        "- Text readability and content accessibility",
        "- Table detection and extraction accuracy",
        "",
        "## 🔑 **PDF RECONCILIATION STRATEGIES**",
        "- Primary reconciliation approach (data-based vs content-based)",
        "- Key matching opportunities across PDF documents",
        "- Cross-reference validation methods",
        "- Similarity analysis techniques for non-tabular content",
        "",
        "## ⚠️ **PDF-SPECIFIC CHALLENGES**",
        "- OCR accuracy and text recognition issues",
        "- Table structure variations and extraction limitations",
        "- Format inconsistencies between PDF documents",
        "- Missing or corrupted data sections",
        "- Document version and timestamp discrepancies",
        "",
        "## 🎯 **RECONCILIATION METHODOLOGY**",
        "- Step-by-step reconciliation process for PDF documents",
        "- Tolerance levels for PDF-based matching",
        "- Exception handling for unreadable sections",
        "- Manual verification checkpoints",
        "- Quality assurance procedures",
        "",
        "## 📊 **IMPACT ASSESSMENT**",
        "- Expected reconciliation accuracy rates",
        "- Manual intervention requirements",
        "- Processing time estimates",
        "- Resource allocation recommendations",
        "",
        "## 🚀 **IMPLEMENTATION PLAN**",
        "- **Phase 1**: PDF preprocessing and standardization",
        "- **Phase 2**: Data extraction and normalization",
        "- **Phase 3**: Cross-document comparison and matching",
        "- **Phase 4**: Exception handling and manual review",
        "- **Phase 5**: Validation and reporting",
        "",
        "## 🛠️ **TECHNICAL RECOMMENDATIONS**",
        "- PDF processing tools and software recommendations",
        "- OCR enhancement strategies",
        "- Automation opportunities and limitations",
        "- Integration with existing systems",
        "",
        "## ⭐ **KEY RECOMMENDATIONS**",
        "- Top 5 critical actions for successful PDF reconciliation",
        "- Risk mitigation strategies",
        "- Success metrics and KPIs",
        "- Continuous improvement opportunities",
        "",
        "Focus on practical, actionable insights specific to PDF document reconciliation. Include specific examples from the actual file data and provide concrete recommendations for handling the unique challenges of PDF-based reconciliation."
    ]
    
    return "\n".join(prompt_sections)

def perform_reconciliation_analysis(files_data):
    """Enhanced reconciliation analysis with PDF-to-PDF focus"""
    if not st.session_state.openai_client:
        st.error("❌ Please connect to OpenAI first")
        return None
    
    try:
        analysis_data = prepare_data_for_analysis(files_data)
        
        if len(analysis_data) < 2:
            st.warning("⚠️ Need at least 2 files with data for reconciliation analysis")
            return None
        
        # Check for PDF-to-PDF reconciliation scenario
        pdf_files = [f for f, data in analysis_data.items() if 'pdf' in data.get('file_type', '').lower()]
        is_pdf_focused = len(pdf_files) >= 2
        
        # Generate appropriate prompt based on file composition
        if is_pdf_focused:
            prompt = generate_pdf_reconciliation_prompt(analysis_data)
            system_message = "You are a senior financial reconciliation expert with 20+ years of experience in PDF document analysis, OCR processing, and cross-document reconciliation. You specialize in extracting insights from complex PDF documents and performing accurate reconciliation across different document formats and structures."
        else:
            # Use general multi-format prompt for mixed scenarios
            prompt = f"""
            You are a senior financial reconciliation expert specializing in multi-format data reconciliation. I have uploaded {len(analysis_data)} files of various formats for comprehensive reconciliation analysis.
            
            Here's the detailed data summary for each file:
            {json.dumps(analysis_data, indent=2, default=str)}
            
            Please provide a COMPREHENSIVE MULTI-FORMAT RECONCILIATION ANALYSIS with detailed sections covering executive summary, data structure analysis, key matching opportunities, data quality assessment, reconciliation strategy, business impact analysis, implementation roadmap, technical recommendations, and key recommendations.
            
            Use specific numbers, percentages, and examples from the actual data. Consider the unique challenges of working with mixed file formats and provide actionable insights for successful reconciliation.
            """
            system_message = "You are a senior financial reconciliation expert with 20+ years of experience in multi-format data analysis, including Excel, PDF, and CSV reconciliation. You specialize in cross-format data integration, format-specific challenges, and enterprise-level reconciliation processes."
        
        with st.spinner("🤖 Performing comprehensive reconciliation analysis..."):
            response = st.session_state.openai_client.chat.completions.create(
                model=st.session_state.model_name,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=4000
            )
        
        return response.choices[0].message.content
        
    except Exception as e:
        st.error(f"❌ Analysis failed: {str(e)}")
        return None

def prepare_data_for_chat(files_data):
    """Enhanced chat data preparation with PDF support"""
    chat_data = {}
    
    for filename, file_info in files_data.items():
        df = file_info['df']
        text_content = file_info['text_content']
        
        file_chat_data = {'filename': filename}
        
        # Handle tabular data
        if df is not None and not df.empty:
            if len(df) <= 1000:
                all_data = df.to_dict('records')
            else:
                all_data = df.head(50).to_dict('records')
            
            file_chat_data.update({
                'has_tabular_data': True,
                'total_rows': len(df),
                'columns': list(df.columns),
                'data_types': df.dtypes.astype(str).to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'unique_counts': df.nunique().to_dict(),
                'sample_data': all_data,
                'summary_stats': df.describe(include='all').fillna('').to_dict() if len(df.select_dtypes(include=['number']).columns) > 0 else {}
            })
        
        # Handle PDF-specific information
        if isinstance(text_content, dict) and text_content.get('type') == 'pdf':
            file_chat_data.update({
                'file_type': 'pdf',
                'structure_analysis': text_content.get('structure_analysis', {}),
                'content_summary': text_content.get('content_summary', {}),
                'has_extractable_tables': text_content.get('has_tables', False),
                'reconciliation_ready': text_content.get('reconciliation_ready', False)
            })
            
            if text_content.get('text_preview'):
                file_chat_data['text_preview'] = text_content['text_preview']
        
        chat_data[filename] = file_chat_data
    
    return chat_data

def chat_with_files(user_question, files_data):
    """Enhanced chat with PDF support using OpenAI"""
    if not st.session_state.openai_client:
        return "❌ Please connect to OpenAI first to use the chat feature."
    
    try:
        chat_data = prepare_data_for_chat(files_data)
        
        if not chat_data:
            return "❌ No data files available for chat. Please upload CSV, Excel, or PDF files."
        
        system_prompt = f"""
        You are an expert data analyst with access to the following uploaded files and their data:
        
        {json.dumps(chat_data, indent=2, default=str)}
        
        Instructions:
        - Answer questions about the data in these files, including PDF documents
        - For PDF files, reference structure analysis, content summaries, and extracted data
        - Provide specific insights, comparisons, and analysis across different file formats
        - Reference actual data points and values when relevant
        - If asked about reconciliation, compare data across files including PDF content
        - For PDF-specific questions, address document structure, content type, and data accessibility
        - Be precise and cite specific numbers/examples from the data
        - If calculations are needed, show your work
        - Format responses clearly with sections when appropriate
        """
        
        messages = [{"role": "system", "content": system_prompt}]
        
        recent_history = st.session_state.chat_history[-10:] if len(st.session_state.chat_history) > 10 else st.session_state.chat_history
        for chat in recent_history:
            messages.append({"role": "user", "content": chat["question"]})
            messages.append({"role": "assistant", "content": chat["answer"]})
        
        messages.append({"role": "user", "content": user_question})
        
        response = st.session_state.openai_client.chat.completions.create(
            model=st.session_state.model_name,
            messages=messages,
            temperature=0.2,
            max_tokens=1500
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        return f"❌ Chat failed: {str(e)}"

def display_chat_interface(files_data):
    """Enhanced chat interface with PDF-specific examples"""
    st.markdown("## 💬 Chat with Your Files")
    st.markdown("Ask questions about your uploaded data files including PDF documents. The AI will analyze and provide insights based on your data.")
    
    if st.session_state.chat_history:
        st.markdown("### 📜 Chat History")
        chat_container = st.container()
        with chat_container:
            for i, chat in enumerate(st.session_state.chat_history):
                st.markdown(f"**🙋 You:** {chat['question']}")
                st.markdown(f"**🤖 AI:** {chat['answer']}")
                st.markdown("---")
    
    st.markdown("### 💭 Ask a Question")
    
    # Enhanced example questions including PDF-specific ones
    with st.expander("💡 Example Questions", expanded=False):
        st.markdown("""
        **General Analysis:**
        - What are the key differences between the uploaded files?
        - How many records are in each file?
        - What columns do the files have in common?
        - Are there any data quality issues I should be aware of?
        
        **PDF-Specific Questions:**
        - What type of documents are in my PDF files?
        - Can you extract and analyze the table data from the PDFs?
        - How does the PDF content compare to my Excel/CSV data?
        - What's the quality of data extraction from the PDF files?
        - Are the PDF documents suitable for reconciliation?
        
        **Reconciliation Analysis:**
        - Can you identify potential matching keys for reconciliation?
        - What discrepancies exist between my files?
        - How should I approach reconciling PDF data with tabular data?
        - What are the challenges in reconciling these specific files?
        """)
    
    with st.form("chat_form", clear_on_submit=True):
        user_question = st.text_area(
            "Your Question:",
            placeholder="e.g., How do the PDF documents compare with the Excel data?",
            height=100
        )
        submitted = st.form_submit_button("💬 Ask Question")
        
        if submitted and user_question.strip():
            with st.spinner("🤔 Analyzing your question..."):
                answer = chat_with_files(user_question, files_data)
                
                # Add to chat history
                st.session_state.chat_history.append({
                    "question": user_question,
                    "answer": answer
                })
                
                # Rerun to show the new chat
                st.rerun()

def perform_automated_reconciliation(files_data):
    """Perform automated reconciliation between files"""
    st.markdown("## 🔄 Automated Reconciliation")
    
    if len(files_data) < 2:
        st.warning("⚠️ Need at least 2 files for reconciliation")
        return
    
    # Let user select files for reconciliation
    file_names = list(files_data.keys())
    
    col1, col2 = st.columns(2)
    with col1:
        file1 = st.selectbox("Select First File", file_names, key="recon_file1")
    with col2:
        available_files = [f for f in file_names if f != file1]
        file2 = st.selectbox("Select Second File", available_files, key="recon_file2")
    
    if st.button("🔍 Start Reconciliation"):
        df1 = files_data[file1]['df']
        df2 = files_data[file2]['df']

        if df1 is None or df2 is None:
            st.error("❌ Both files must contain extractable data for reconciliation")
            return
        
        with st.spinner("🔄 Performing automated reconciliation..."):
            try:
                # Find common columns
                common_cols = list(set(df1.columns) & set(df2.columns))
                
                if not common_cols:
                    st.error("❌ No common columns found between the files")
                    return
                
                st.success(f"✅ Found {len(common_cols)} common columns: {', '.join(common_cols)}")
                
                # Perform basic reconciliation
                results = {}
                
                for col in common_cols:
                    # Convert to string for comparison
                    df1_values = set(df1[col].astype(str).dropna())
                    df2_values = set(df2[col].astype(str).dropna())
                    
                    matches = df1_values & df2_values
                    only_in_file1 = df1_values - df2_values
                    only_in_file2 = df2_values - df1_values
                    
                    results[col] = {
                        'matches': len(matches),
                        'only_in_file1': len(only_in_file1),
                        'only_in_file2': len(only_in_file2),
                        'match_rate': len(matches) / max(len(df1_values), len(df2_values)) * 100 if max(len(df1_values), len(df2_values)) > 0 else 0
                    }
                
                # Display results
                st.markdown("### 📊 Reconciliation Results")
                
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                total_matches = sum(r['matches'] for r in results.values())
                total_discrepancies = sum(r['only_in_file1'] + r['only_in_file2'] for r in results.values())
                avg_match_rate = sum(r['match_rate'] for r in results.values()) / len(results) if results else 0
                
                with col1:
                    st.metric("Total Matches", total_matches)
                with col2:
                    st.metric("Total Discrepancies", total_discrepancies)
                with col3:
                    st.metric("Avg Match Rate", f"{avg_match_rate:.1f}%")
                with col4:
                    st.metric("Columns Compared", len(common_cols))
                
                # Detailed results
                st.markdown("### 📋 Detailed Column Analysis")
                results_df = pd.DataFrame(results).T
                results_df.index.name = 'Column'
                results_df = results_df.reset_index()
                results_df['match_rate'] = results_df['match_rate'].round(2)
                st.dataframe(results_df, use_container_width=True)
                
                # Store results for later use
                st.session_state.reconciliation_results[f"{file1}_vs_{file2}"] = results
                
            except Exception as e:
                st.error(f"❌ Reconciliation failed: {str(e)}")

def main():
    """Main application function"""
    st.set_page_config(
        page_title="PDF & Data Reconciliation Tool",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for better styling
    st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 5px;
        border-left: 5px solid #667eea;
    }
    .file-upload-section {
        border: 2px dashed #667eea;
        border-radius: 10px;
        padding: 2rem;
        text-align: center;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>📊 Advanced PDF & Data Reconciliation Tool</h1>
        <p>Intelligent document analysis and multi-format data reconciliation with AI-powered insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Setup OpenAI connection
    openai_connected = setup_openai_client()
    
    # Navigation
    selected = option_menu(
        menu_title=None,
        options=["📁 File Upload", "🔍 Analysis", "🔄 Reconciliation", "💬 AI Chat"],
        icons=["cloud-upload", "search", "arrows-clockwise", "chat-dots"],
        menu_icon="cast",
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "background-color": "#fafafa"},
            "icon": {"color": "#667eea", "font-size": "18px"},
            "nav-link": {
                "font-size": "16px",
                "text-align": "center",
                "margin": "0px",
                "--hover-color": "#eee"
            },
            "nav-link-selected": {"background-color": "#667eea"},
        }
    )
    
    if selected == "📁 File Upload":
        st.markdown("## 📁 Upload Your Files")
        st.markdown("Upload CSV, Excel, or PDF files for analysis and reconciliation. The tool supports multiple file formats and advanced PDF processing.")
        
        # File upload section
        st.markdown('<div class="file-upload-section">', unsafe_allow_html=True)
        uploaded_files = st.file_uploader(
            "Choose files",
            accept_multiple_files=True,
            type=['csv', 'xlsx', 'xls', 'pdf'],
            help="Supported formats: CSV, Excel (.xlsx, .xls), and PDF files"
        )
        st.markdown('</div>', unsafe_allow_html=True)
        
        if uploaded_files:
            st.markdown("### 📊 Processing Files...")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, uploaded_file in enumerate(uploaded_files):
                progress = (i + 1) / len(uploaded_files)
                progress_bar.progress(progress)
                status_text.text(f"Processing {uploaded_file.name}...")
                
                # Process file
                df, text_content = read_file(uploaded_file)
                
                # Store in session state
                st.session_state.processed_files[uploaded_file.name] = {
                    'df': df,
                    'text_content': text_content,
                    'upload_time': pd.Timestamp.now()
                }
            
            progress_bar.progress(1.0)
            status_text.text("✅ All files processed successfully!")
            
            # Display file summary
            st.markdown("### 📋 Uploaded Files Summary")
            
            summary_data = []
            for filename, file_info in st.session_state.processed_files.items():
                df = file_info['df']
                text_content = file_info['text_content']
                
                file_type = filename.split('.')[-1].upper()
                rows = len(df) if df is not None else 0
                cols = len(df.columns) if df is not None else 0
                
                # Handle PDF-specific information
                status = "✅ Ready"
                if isinstance(text_content, dict) and text_content.get('type') == 'pdf':
                    if text_content.get('reconciliation_ready'):
                        status = "✅ PDF Data Extracted"
                    else:
                        status = "⚠️ PDF Processed (Limited Data)"
                elif text_content and 'error' in str(text_content):
                    status = "❌ Processing Error"
                
                summary_data.append({
                    'Filename': filename,
                    'Type': file_type,
                    'Rows': rows,
                    'Columns': cols,
                    'Status': status,
                    'Upload Time': file_info['upload_time'].strftime('%H:%M:%S')
                })
            
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True)
            
        else:
            st.info("👆 Upload files to get started with analysis and reconciliation")
    
    elif selected == "🔍 Analysis":
        st.markdown("## 🔍 File Analysis")
        
        if not st.session_state.processed_files:
            st.warning("⚠️ Please upload files first in the 'File Upload' section")
            return
        
        # File selection for detailed analysis
        selected_file = st.selectbox(
            "Select a file for detailed analysis:",
            list(st.session_state.processed_files.keys())
        )
        
        if selected_file:
            file_info = st.session_state.processed_files[selected_file]
            df = file_info['df']
            text_content = file_info['text_content']
            
            # Display analysis based on file type
            if df is not None and not df.empty:
                if isinstance(text_content, dict) and text_content.get('type') == 'pdf':
                    display_enhanced_pdf_analysis(text_content, selected_file)
                    if text_content.get('has_tables'):
                        st.markdown("### 📊 Extracted Table Data")
                        display_csv_analysis(df, selected_file)
                else:
                    display_csv_analysis(df, selected_file)
            elif isinstance(text_content, dict) and text_content.get('type') == 'pdf':
                display_enhanced_pdf_analysis(text_content, selected_file)
            else:
                st.error(f"❌ Unable to analyze {selected_file}: {text_content}")
        
        # AI-Powered Analysis Section
        if openai_connected and st.session_state.processed_files:
            st.markdown("---")
            st.markdown("## 🤖 AI-Powered Reconciliation Analysis")
            st.markdown("Get comprehensive insights and reconciliation strategies powered by AI")
            
            if st.button("🚀 Generate AI Analysis", type="primary"):
                analysis_result = perform_reconciliation_analysis(st.session_state.processed_files)
                if analysis_result:
                    st.markdown("### 📈 AI Reconciliation Analysis")
                    st.markdown(analysis_result)
                    
                    # Store analysis in session state
                    st.session_state.reconciliation_results['ai_analysis'] = analysis_result
    
    elif selected == "🔄 Reconciliation":
        st.markdown("## 🔄 Data Reconciliation")
        
        if not st.session_state.processed_files:
            st.warning("⚠️ Please upload files first in the 'File Upload' section")
            return
        
        # Show available reconciliation options
        files_with_data = {k: v for k, v in st.session_state.processed_files.items() 
                          if v['df'] is not None and not v['df'].empty}
        
        if len(files_with_data) < 2:
            st.warning("⚠️ Need at least 2 files with extractable data for reconciliation")
            
            # Show file status
            st.markdown("### 📋 File Status for Reconciliation")
            for filename, file_info in st.session_state.processed_files.items():
                df = file_info['df']
                text_content = file_info['text_content']
                
                if df is not None and not df.empty:
                    st.success(f"✅ {filename} - Ready for reconciliation ({len(df)} rows)")
                elif isinstance(text_content, dict) and text_content.get('type') == 'pdf':
                    if text_content.get('reconciliation_ready'):
                        st.info(f"⚠️ {filename} - PDF with limited data extraction")
                    else:
                        st.warning(f"⚠️ {filename} - PDF requires manual processing")
                else:
                    st.error(f"❌ {filename} - No extractable data")
        else:
            perform_automated_reconciliation(files_with_data)
            
            # Show previous reconciliation results
            if st.session_state.reconciliation_results:
                st.markdown("---")
                st.markdown("### 📊 Previous Reconciliation Results")
                
                for result_key, result_data in st.session_state.reconciliation_results.items():
                    if result_key != 'ai_analysis':
                        with st.expander(f"📋 {result_key}"):
                            if isinstance(result_data, dict):
                                results_df = pd.DataFrame(result_data).T
                                st.dataframe(results_df)
    
    elif selected == "💬 AI Chat":
        if not openai_connected:
            st.warning("⚠️ Please connect to OpenAI in the sidebar to use the chat feature")
            return
        
        if not st.session_state.processed_files:
            st.warning("⚠️ Please upload files first to chat about your data")
            return
        
        display_chat_interface(st.session_state.processed_files)
    
    # Sidebar information
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 📊 Session Summary")
        if st.session_state.processed_files:
            st.metric("Files Uploaded", len(st.session_state.processed_files))
            
            # Count file types
            file_types = {}
            for filename in st.session_state.processed_files.keys():
                ext = filename.split('.')[-1].upper()
                file_types[ext] = file_types.get(ext, 0) + 1
            
            for file_type, count in file_types.items():
                st.metric(f"{file_type} Files", count)
            
            if st.session_state.reconciliation_results:
                st.metric("Analyses Done", len(st.session_state.reconciliation_results))
        else:
            st.info("No files uploaded yet")
        
        st.markdown("---")
        st.markdown("### ℹ️ About")
        st.markdown("""
        This tool provides:
        - **Multi-format support**: CSV, Excel, PDF
        - **Advanced PDF processing**: Structure analysis and data extraction
        - **AI-powered insights**: Intelligent reconciliation strategies
        - **Interactive chat**: Ask questions about your data
        - **Automated reconciliation**: Cross-file comparison and matching
        """)
        
        if st.button("🗑️ Clear All Data"):
            for key in ['processed_files', 'reconciliation_results', 'chat_history']:
                if key in st.session_state:
                    st.session_state[key] = {} if key != 'chat_history' else []
            st.success("✅ All data cleared!")
            st.rerun()

if __name__ == "__main__":
    main()