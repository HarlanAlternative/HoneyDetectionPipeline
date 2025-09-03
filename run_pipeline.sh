#!/bin/bash

# Unified Pipeline Runner
# Combines all pipeline functionality into one script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}$1${NC}"
}

# Function to check dependencies
check_dependencies() {
    print_status "Checking dependencies..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python3 not found. Please install Python 3.8+"
        exit 1
    fi
    
    # Check required files
    if [ ! -f "run_integrated_pipeline.py" ]; then
        print_error "Integrated pipeline script not found"
        exit 1
    fi
    
    if [ ! -f "src/unified_powerbi_generator.py" ]; then
        print_error "PowerBI generator script not found"
        exit 1
    fi
    
    print_status "All dependencies satisfied"
}

# Function to show help
show_help() {
    cat << EOF
Unified Pipeline Runner - Honey Quality Analysis System

Usage: $0 [OPTION]

Options:
    -h, --help          Show this help message
    -f, --full          Run complete industrial pipeline (default)
    -p, --powerbi       Generate PowerBI reports only
    -e, --etl           Run ETL pipeline only
    -d, --demo          Run quick demo
    -c, --clean         Clean up generated files
    -s, --status        Show system status

Examples:
    $0                  # Run complete pipeline
    $0 --powerbi       # Generate PowerBI reports only
    $0 --demo          # Run quick demo
    $0 --clean         # Clean up files

EOF
}

# Function to run complete pipeline
run_complete_pipeline() {
    print_header "Running Complete Industrial Pipeline"
    print_status "This will run the complete pipeline with:"
    echo "  - ETL Data Processing"
    echo "  - PowerBI Templates"
    echo "  - Comprehensive PowerBI Reports"
    echo "  - Excel Dashboards"
    echo ""
    
    python3 run_integrated_pipeline.py
    
    if [ $? -eq 0 ]; then
        print_status "Pipeline completed successfully!"
        show_results
    else
        print_error "Pipeline execution failed"
        exit 1
    fi
}

# Function to generate PowerBI reports only
generate_powerbi_reports() {
    print_header "Generating PowerBI Reports"
    print_status "This will generate:"
    echo "  - Excel reports with charts"
    echo "  - PowerBI-ready data files"
    echo "  - PowerBI templates and guides"
    echo ""
    
    python3 src/unified_powerbi_generator.py
    
    if [ $? -eq 0 ]; then
        print_status "PowerBI reports generated successfully!"
        show_powerbi_results
    else
        print_error "PowerBI report generation failed"
        exit 1
    fi
}

# Function to run ETL only
run_etl_only() {
    print_header "Running ETL Pipeline Only"
    print_status "This will run data extraction, transformation, and loading"
    echo ""
    
    # Create a simple ETL runner
    python3 -c "
from src.industrial_etl import IndustrialETLProcessor
import pandas as pd
import numpy as np

# Generate sample data
np.random.seed(42)
data = pd.DataFrame({
    'batch_id': [f'BATCH_{i:03d}' for i in range(1, 101)],
    'moisture': np.random.uniform(15.0, 20.0, 100),
    'ph': np.random.uniform(3.5, 6.5, 100),
    'diastase_activity': np.random.uniform(8.0, 15.0, 100),
    'h_m_f': np.random.uniform(20.0, 40.0, 100)
})

# Save sample data
data.to_csv('data/sample_honey_data.csv', index=False)
print('Sample data generated and saved')

# Initialize ETL processor
etl = IndustrialETLProcessor()
result = etl.run_etl_pipeline('data/sample_honey_data.csv', 'honey_quality_data')
print(f'ETL result: {result}')
"
    
    if [ $? -eq 0 ]; then
        print_status "ETL pipeline completed successfully!"
    else
        print_error "ETL pipeline failed"
        exit 1
    fi
}

# Function to run quick demo
run_demo() {
    print_header "Running Quick Demo"
    print_status "This will run a simplified demo of the pipeline"
    echo ""
    
    python3 -c "
from src.unified_powerbi_generator import UnifiedPowerBIGenerator

# Run quick demo
generator = UnifiedPowerBIGenerator()
results = generator.generate_all_reports(100)
print('Demo completed successfully!')
print(f'Generated {results[\"data_summary\"][\"total_samples\"]} samples')
"
    
    if [ $? -eq 0 ]; then
        print_status "Demo completed successfully!"
    else
        print_error "Demo failed"
        exit 1
    fi
}

# Function to clean up files
cleanup_files() {
    print_header "Cleaning Up Generated Files"
    
    read -p "Are you sure you want to delete all generated files? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Removing generated files..."
        
        # Remove PowerBI reports
        if [ -d "powerbi_reports" ]; then
            rm -rf powerbi_reports/*
            print_status "PowerBI reports cleaned"
        fi
        
        # Remove PowerBI templates
        if [ -d "powerbi_templates" ]; then
            rm -rf powerbi_templates/*
            print_status "PowerBI templates cleaned"
        fi
        
        # Remove output files
        if [ -d "output" ]; then
            rm -rf output/*
            print_status "Output files cleaned"
        fi
        
        # Remove data files
        if [ -d "data" ]; then
            rm -f data/sample_honey_data.csv
            print_status "Data files cleaned"
        fi
        
        print_status "Cleanup completed"
    else
        print_status "Cleanup cancelled"
    fi
}

# Function to show system status
show_status() {
    print_header "System Status"
    
    echo "Python Version: $(python3 --version 2>&1)"
    echo "Working Directory: $(pwd)"
    echo ""
    
    echo "Generated Files:"
    if [ -d "powerbi_reports" ]; then
        echo "  PowerBI Reports: $(ls powerbi_reports/ | wc -l) files"
    else
        echo "  PowerBI Reports: Directory not found"
    fi
    
    if [ -d "powerbi_templates" ]; then
        echo "  PowerBI Templates: $(ls powerbi_templates/ | wc -l) files"
    else
        echo "  PowerBI Templates: Directory not found"
    fi
    
    if [ -d "data" ]; then
        echo "  Data Files: $(ls data/ | wc -l) files"
    else
        echo "  Data Files: Directory not found"
    fi
    
    if [ -d "output" ]; then
        echo "  Output Files: $(ls output/ | wc -l) files"
    else
        echo "  Output Files: Directory not found"
    fi
}

# Function to show results
show_results() {
    echo ""
    print_header "Pipeline Results"
    echo "Generated Outputs:"
    
    if [ -d "powerbi_reports" ]; then
        echo "  PowerBI Reports: $(ls powerbi_reports/ | wc -l) files"
    fi
    
    if [ -d "powerbi_templates" ]; then
        echo "  PowerBI Templates: $(ls powerbi_templates/ | wc -l) files"
    fi
    
    if [ -d "data" ]; then
        echo "  Data Files: $(ls data/ | wc -l) files"
    fi
    
    echo ""
    print_status "Next Steps:"
    echo "1. Open Excel reports for detailed analysis"
    echo "2. View HTML dashboard preview"
    echo "3. Import data into PowerBI Desktop"
    echo "4. Create custom PowerBI dashboards"
}

# Function to show PowerBI results
show_powerbi_results() {
    echo ""
    print_header "PowerBI Results"
    
    if [ -d "powerbi_reports" ]; then
        echo "Generated Files:"
        ls -1 powerbi_reports/ | head -5 | sed 's/^/  - /'
        if [ $(ls powerbi_reports/ | wc -l) -gt 5 ]; then
            echo "  ... and $(($(ls powerbi_reports/ | wc -l) - 5)) more files"
        fi
    fi
    
    echo ""
    print_status "Next Steps:"
    echo "1. Open Excel report for immediate analysis"
    echo "2. Import CSV data into PowerBI Desktop"
    echo "3. Follow the template guide"
    echo "4. Use DAX measures for calculations"
}

# Main function
main() {
    # Parse command line arguments
    case "${1:-}" in
        -h|--help)
            show_help
            exit 0
            ;;
        -f|--full)
            check_dependencies
            run_complete_pipeline
            ;;
        -p|--powerbi)
            check_dependencies
            generate_powerbi_reports
            ;;
        -e|--etl)
            check_dependencies
            run_etl_only
            ;;
        -d|--demo)
            check_dependencies
            run_demo
            ;;
        -c|--clean)
            cleanup_files
            ;;
        -s|--status)
            show_status
            ;;
        "")
            # Default: run complete pipeline
            check_dependencies
            run_complete_pipeline
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
