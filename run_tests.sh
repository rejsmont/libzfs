#!/usr/bin/env bash
# Test runner script for libzfseasy

set -e

echo "🧪 Running libzfseasy test suite"
echo "================================"
echo ""

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${RED}Error: pytest is not installed${NC}"
    echo "Install it with: poetry install --with dev"
    exit 1
fi

# Parse command line arguments
RUN_UNIT=false
RUN_INTEGRATION=false
RUN_ALL=true
VERBOSE=""
COVERAGE=true
PARALLEL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --unit)
            RUN_UNIT=true
            RUN_ALL=false
            shift
            ;;
        --integration)
            RUN_INTEGRATION=true
            RUN_ALL=false
            shift
            ;;
        --no-coverage)
            COVERAGE=false
            shift
            ;;
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        -vv)
            VERBOSE="-vv"
            shift
            ;;
        -p|--parallel)
            PARALLEL="-n auto"
            shift
            ;;
        --help|-h)
            echo "Usage: ./run_tests.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --unit           Run only unit tests"
            echo "  --integration    Run only integration tests"
            echo "  --no-coverage    Skip coverage report"
            echo "  -v, --verbose    Verbose output"
            echo "  -vv              Very verbose output"
            echo "  -p, --parallel   Run tests in parallel"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./run_tests.sh                    # Run all tests with coverage"
            echo "  ./run_tests.sh --unit             # Run only unit tests"
            echo "  ./run_tests.sh --parallel -v      # Run tests in parallel with verbose output"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build pytest command
PYTEST_CMD="pytest"

if [ "$VERBOSE" != "" ]; then
    PYTEST_CMD="$PYTEST_CMD $VERBOSE"
fi

if [ "$PARALLEL" != "" ]; then
    PYTEST_CMD="$PYTEST_CMD $PARALLEL"
fi

if [ "$COVERAGE" = true ]; then
    PYTEST_CMD="$PYTEST_CMD --cov=libzfseasy --cov-report=term-missing --cov-report=html"
fi

# Add markers based on flags
if [ "$RUN_UNIT" = true ]; then
    echo -e "${YELLOW}Running unit tests only${NC}"
    PYTEST_CMD="$PYTEST_CMD -m unit"
elif [ "$RUN_INTEGRATION" = true ]; then
    echo -e "${YELLOW}Running integration tests only${NC}"
    PYTEST_CMD="$PYTEST_CMD -m integration"
else
    echo -e "${YELLOW}Running all tests${NC}"
fi

echo ""

# Run tests
if eval $PYTEST_CMD; then
    echo ""
    echo -e "${GREEN}✓ All tests passed!${NC}"
    
    if [ "$COVERAGE" = true ]; then
        echo ""
        echo -e "${GREEN}Coverage report generated in htmlcov/index.html${NC}"
    fi
    
    exit 0
else
    echo ""
    echo -e "${RED}✗ Tests failed${NC}"
    exit 1
fi
