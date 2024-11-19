#!/bin/bash
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

VERSION='1.3.0'

# ==============================================================================
# Script Name: Tinybird Data Source and Pipe Management
# Description: This script manages Tinybird data sources and pipes. It can
#              truncate data sources and populate pipes based on configurable
#              inclusion and exclusion criteria.
# Author:      Sebastian Zaklada
#
# Usage:       . scripts/repopulateAllDataSources.sh [COMMAND]
#
# Commands:
#              dryrun     - Performs a dry run without mutating any data sources
#              repopulate - DESTRUCTIVE OPERATION! Truncates and populates 
#                           matching data sources
#
# Example:     . scripts/repopulateAllDataSources.sh dryrun
#              . scripts/repopulateAllDataSources.sh repopulate
#
# Note:        You must be in /tinybird folder for this script to be able to 
#              use tb authentication details
#
# !            ⚠️ WARNING ⚠️
# !            Remember to set your workspace before running this script
# !            e.g. tb workspace use dev
# !
# !            DO NOT USE UNLESS YOU KNOW WHAT YOU ARE DOING!
# !            WITH DEFAULT SETTINGS THIS SCRIPT WILL TRUNCATE ALL DATA SOURCES 
# !            CAUSING AN OUTAGE UNTIL THE RE-POPULATION PROCESS FINISHES 
# !            PROCESSING INGESTION IN ALL ROOT LEVEL AND DEPENDING DATA SOURCES
#
# Dependencies:
#   - jq (for JSON parsing)
#   - Tinybird CLI (tb command)
#
# Notes:
#   - The script will run in dry-run mode by default
#   - Modify the whitelist and exclusion arrays as needed
# ==============================================================================

## Color definitions
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
LIGHTBLUE='\033[0;94m' 
PURPLE='\033[0;35m'
LIGHTPURPLE='\033[1;35m'
NC='\033[0m' # No Color
BOLD='\033[1m'
BOLD_OFF='\033[22m' # This turns off bold
UNDERLINE='\033[4m'
UNDERLINE_OFF='\033[24m' # This turns off just the underline
POWERLINE_TB=$'\uf1c0'

## ! DO NOT EDIT THIS LINE
DEFAULT_DATASOURCE_PREFIX_EXCLUDE=("source_" "ops_" "snapshot_")

## Dry Run flag
isDryRun=true # Set to false for actual execution

## Configuration arrays for data sources
dataSourcePrefixExclude=() # Additional prefixes to exclude (defaults are always included)
dataSourceNameExclude=() # Leave empty to process all data sources
dataSourceWhitelist=("cdc_PurchaseOrdersWithItems" "agg_PurchaseOrderWithItems") # For testing - leave empty to process all non-excluded data sources

pipeIncludePrefixes=("etl_PurchaseOrdersWithItems") # Pipes starting with these prefixes will be processed
priorityPipes=("ingest_RateTemplates" "ingest_Projects" "ingest_Users" "ingest_LaborClasses" "ingest_COR" "ingest_PurchaseOrders") # Pipes to be processed first
# finalPipes=("etl_TimeWithCost") # Pipes to be processed last
finalPipes=() # Pipes to be processed last
pipeExclude=() # Pipes to be excluded even if they match prefix or are in whitelist
pipeWhitelist=() # Specific pipes to be processed regardless of prefix

## Configuration overrides for smaller blast radius re-population activities
# dataSourceOverridePrefixes=("cdc_Rates" "cdc_TimeWith") # Only these will be processed
# pipeOverridePrefixes=("etl_Rates" "etl_TimeWithRateTemplates") # Only these will be processed

print_usage() {
    echo -e "\n${BOLD}Tinybird Datasource Population Tool v${VERSION}${NC}"
    echo -e "Copyright (c) 2024 Sebastian Zaklada | eSUB Inc.\nMIT License - Use and modify freely. Full license at: https://opensource.org/licenses/MIT${NC}\n"

    echo -e "${BOLD}Usage:${BOLD_OFF} ./repopulateAllDataSources.sh ${UNDERLINE}COMMAND${UNDERLINE_OFF}\n"

    echo -e "${BOLD}Commands:${BOLD_OFF}"
    echo -e "  ${GREEN}dryrun${NC}         Performs a dry run without mutating any data sources"
    echo -e "  ${PURPLE}repopulate${NC}     ${YELLOW}DESTRUCTIVE OPERATION!${NC} Truncates and populates matching data sources\n"

    echo -e "${BOLD}Examples:${BOLD_OFF}"
    echo -e "  ${BLUE}. scripts/repopulateAllDataSources.sh dryrun${NC}"
    echo -e "  ${BLUE}. scripts/repopulateAllDataSources.sh repopulate${NC}\n"

    echo -e "${YELLOW}${BOLD}Note:${BOLD_OFF} Ensure you are in the /tinybird folder before running this script${NC}"
    echo -e "${YELLOW}${BOLD}Warning:${BOLD_OFF} Set your workspace before running (e.g., ${UNDERLINE}tb workspace use dev${UNDERLINE_OFF})${NC}\n"
}

check_response() {
    local response
    read -r response
    case "$response" in
        [yY][eE][sS]|[yY]|"") 
            return 0
            ;;
        *)            
            return 1
            ;;
    esac
}

# Display current configuration and ask for confirmation
confirm() {        
    echo -e "\n${BOLD}Tinybird Datasource Population Tool v${VERSION}${NC}"
    echo -e "Copyright (c) 2024 Sebastian Zaklada | eSUB Inc.\nMIT License - Use and modify freely. Full license at: https://opensource.org/licenses/MIT${NC}\n"

    # Display configuration
    echo -e "${BOLD}Data Source Prefix Exclusions:${NC}"
    printf '  %s\n' "${dataSourcePrefixExclude[@]}"

    echo -e "\n${BOLD}Data Source Whitelist:${NC}"
    if [ ${#dataSourceWhitelist[@]} -eq 0 ]; then
        echo "  (empty)"
    else
        printf '  %s\n' "${dataSourceWhitelist[@]}"
    fi

    echo -e "\n${BOLD}Data Source Exclude List:${NC}"
    if [ ${#dataSourceNameExclude[@]} -eq 0 ]; then
        echo "  (empty)"
    else
        printf '  %s\n' "${dataSourceNameExclude[@]}"
    fi

    echo -e "\n${BOLD}Pipe Include Prefixes:${NC}"
    printf '  %s\n' "${pipeIncludePrefixes[@]}"

    echo -e "\n${BOLD}Pipe Whitelist:${NC}"
    if [ ${#pipeWhitelist[@]} -eq 0 ]; then
        echo "  (empty)"
    else
        printf '  %s\n' "${pipeWhitelist[@]}"
    fi

    echo -e "\n${BOLD}Pipe Exclude List:${NC}"
    if [ ${#pipeExclude[@]} -eq 0 ]; then
        echo "  (empty)"
    else
        printf '  %s\n' "${pipeExclude[@]}"
    fi

    echo -e "\n${BOLD}Priority Pipes:${NC}"
    if [ ${#priorityPipes[@]} -eq 0 ]; then
        echo "  (empty)"
    else
        printf '  %s\n' "${priorityPipes[@]}"
    fi

    echo -e "\n${BOLD}Final Pipes:${NC}"
    if [ ${#finalPipes[@]} -eq 0 ]; then
        echo "  (empty)"
    else
        printf '  %s\n' "${finalPipes[@]}"
    fi

    if [ ${#dataSourceOverridePrefixes[@]} -ne 0 ]; then
        echo -e "\n${BOLD}Data Source Override Prefixes:${NC}"
        printf '  %s\n' "${dataSourceOverridePrefixes[@]}"
    fi

    if [ ${#pipeOverridePrefixes[@]} -ne 0 ]; then
        echo -e "\n${BOLD}Pipe Override Prefixes:${NC}"
        printf '  %s\n' "${pipeOverridePrefixes[@]}"
    fi

    ## ! Safeguards !
    # Print testing mode status
    if [ "$isDryRun" = true ]; then
        echo -e "\n${YELLOW}${BOLD}🔍  DRY RUN MODE\n\n${YELLOW}${BOLD_OFF}Simulating operations. No actual changes will be made.${NC}"
    else
        echo -e "\n${PURPLE}${BOLD}⚠️  REPOPULATE MODE\n\nCAUTION: ${BOLD_OFF}${PURPLE}Live operation. All changes will be applied to the target Tinybird workspace!${NC}"
        echo -e "${YELLOW}Carefully review your current workspace and script settings listed above.${NC}"
        echo -e "There is no undo. There is also no spoon 🥄  ${UNDERLINE}https://t.ly/YaheG${NC}"
    fi
    echo -en "\n${BLUE}${BOLD}Have you thoroughly checked ${UNDERLINE}ALL${UNDERLINE_OFF} the above settings? [y/N] ${NC}"    
    check_response || return 1

    echo -en "\nRetrieving current workspace information...${NC}\n"    
    if [ -e ".tinyb" ]; then        
        branch_name=`grep '"name":' .tinyb | cut -d : -f 2 | cut -d '"' -f 2`
        region=`grep '"host":' .tinyb | cut -d / -f 3 | cut -d . -f 2 | cut -d : -f 1`
        if [ "$region" = "tinybird" ]; then
        region=`grep '"host":' .tinyb | cut -d / -f 3 | cut -d . -f 1`
        fi                    
    fi
    tb workspace current    
    echo -en "\n${BLUE}${BOLD}Have you confirmed that your current workspace ${NC}$POWERLINE_TB tb:${branch_name}@${region}${BLUE}${BOLD} matches the intended target workspace in Tinybird? [y/N] ${NC}"    
    check_response || return 1

    echo -en "${BLUE}${BOLD}Have you performed at least one dry run? [y/N] ${NC}"
    check_response || return 1

    if [ "$isDryRun" != true ]; then
        echo -e "\n${YELLOW}WARNING: This is a destructive operation. Proceeding may result in data loss.${NC}"
        echo -e "\n${YELLOW}The following actions will occur:${NC}"
        echo -e "${YELLOW}- Matching data sources will be truncated${NC}"
        echo -e "${YELLOW}- Data population from matching pipes will be initiated${NC}"
        echo -e "${YELLOW}- Downstream pipes will update data with eventual consistency${NC}"
    fi

    echo -en "\n${BLUE}${BOLD}Are you ${UNDERLINE}ABSOLUTELY${UNDERLINE_OFF} certain you want to proceed? [y/N] ${NC}"
    check_response || return 1
    
    return 0
}

combine_exclusions() {
    local combined=("${DEFAULT_DATASOURCE_PREFIX_EXCLUDE[@]}" "${dataSourcePrefixExclude[@]}")
    dataSourcePrefixExclude=("${combined[@]}")
}

# Check if a string starts with any prefix from an array
startsWithPrefix() {
    local str="$1"
    shift
    local arr=("$@")
    for prefix in "${arr[@]}"; do
        if [[ "$str" == "$prefix"* ]]; then
            return 0
        fi
    done
    return 1
}

# Check if a string is in an array
isInArray() {
    local str="$1"
    shift
    local arr=("$@")
    for item in "${arr[@]}"; do
        if [[ "$str" == "$item" ]]; then
            return 0
        fi
    done
    return 1
}

# Process data sources
# Essentially it takes care of truncating only the data sources that should be truncated prior to ingesting data
process_data_sources() {
    echo -e "\n${BOLD}⚙️  Processing data sources${NC}\n"
    datasources_json=$(tb datasource ls --format json)

    # First, list all skipped data sources
    echo -e "${YELLOW}${BOLD}Skipped data sources:${NC}"
    echo "$datasources_json" | jq -c '.datasources[]' | while read -r datasource; do
        name=$(echo "$datasource" | jq -r '.name')
        
        if [ ${#dataSourceOverridePrefixes[@]} -ne 0 ]; then
            if ! startsWithPrefix "$name" "${dataSourceOverridePrefixes[@]}"; then
                echo -e "${YELLOW}Skipping ${BOLD}$name${NC} (not matching override prefixes)${NC}"
                continue
            fi
        else
            if [ ${#dataSourceWhitelist[@]} -ne 0 ] && ! isInArray "$name" "${dataSourceWhitelist[@]}"; then
                echo -e "${YELLOW}Skipping ${BOLD}$name${NC} (not in whitelist)${NC}"
                continue
            fi

            if startsWithPrefix "$name" "${dataSourcePrefixExclude[@]}" || isInArray "$name" "${dataSourceNameExclude[@]}"; then
                echo -e "${YELLOW}Skipping ${BOLD}$name${NC} (excluded)${NC}"
                continue
            fi
        fi
    done

    # Then, process included data sources
    echo -e "\n${BOLD}⚙️  Processing included data sources${NC}"
    echo "$datasources_json" | jq -c '.datasources[]' | while read -r datasource; do
        name=$(echo "$datasource" | jq -r '.name')
        
        if [ ${#dataSourceOverridePrefixes[@]} -ne 0 ]; then
            if ! startsWithPrefix "$name" "${dataSourceOverridePrefixes[@]}"; then
                continue
            fi
        else
            if [ ${#dataSourceWhitelist[@]} -ne 0 ] && ! isInArray "$name" "${dataSourceWhitelist[@]}"; then
                continue
            fi

            if startsWithPrefix "$name" "${dataSourcePrefixExclude[@]}" || isInArray "$name" "${dataSourceNameExclude[@]}"; then
                continue
            fi
        fi

        echo -e "Processing data source ${BOLD}$name${NC}"
        if [ "$isDryRun" = true ]; then
            echo -e "${YELLOW}${BOLD}TESTING - Would truncate ${BOLD}$name${NC}"
        else
            if tb datasource truncate "$name" --yes; then
                echo -e "${GREEN}Successfully truncated ${BOLD}$name${NC}"
            else
                echo -e "${RED}Error truncating ${BOLD}$name${NC}"
            fi
        fi
    done
}

# Process all pipes that should be processed per script configuration
process_pipes() {
    echo -e "\n${BOLD}⚙️  Processing pipes${NC}\n"
    pipes_json=$(tb pipe ls --format json)

    # Function to check if a pipe should be processed
    should_process_pipe() {
        local name=$1
        if [ ${#pipeOverridePrefixes[@]} -ne 0 ]; then
            startsWithPrefix "$name" "${pipeOverridePrefixes[@]}"
        else
            if [ ${#pipeWhitelist[@]} -ne 0 ]; then
                isInArray "$name" "${pipeWhitelist[@]}"
            else
                startsWithPrefix "$name" "${pipeIncludePrefixes[@]}"
            fi
        fi
    }

    # Processes a single pipe
    # Populates data source(s) tied to this pipe from the data sourced off the pipe itself
    process_single_pipe() {
        local name=$1
        local type=$2
        echo -e "Processing ${type} pipe ${BOLD}$name ${NC}"
        if [ "$isDryRun" = true ]; then
            echo -e "${YELLOW}${BOLD}TESTING - ${NC}Would populate ${type} pipe ${BOLD}$name${NC}"
        else
            if tb pipe populate "$name" --wait --truncate; then
                echo -e "${GREEN}Successfully populated ${type} pipe: ${BOLD}$name${NC}"
            else
                echo -e "${RED}Error populating ${type} pipe: ${BOLD}$name${NC}"
            fi
        fi
    }

    # First, list all skipped pipes
    echo -e "${YELLOW}${BOLD}Skipped pipes:${NC}"
    echo "$pipes_json" | jq -c '.pipes[]' | while read -r pipe; do
        name=$(echo "$pipe" | jq -r '.name')
        if ! should_process_pipe "$name" || isInArray "$name" "${pipeExclude[@]}"; then
            echo -e "${YELLOW}Skipping ${BOLD}$name${NC}"
        fi
    done

    # Process priority pipes first
    echo -e "\n${BOLD}⚙️  Processing priority pipes${NC}\n"
    for name in "${priorityPipes[@]}"; do
        if should_process_pipe "$name" && ! isInArray "$name" "${pipeExclude[@]}"; then
            process_single_pipe "$name" "priority"
        else
            echo -e "${YELLOW}Skipping priority pipe ${BOLD}$name${NC} (excluded or not matching criteria)${NC}"
        fi
    done

    # Process regular pipes
    echo -e "\n${BOLD}⚙️  Processing regular pipes${NC}\n"
    echo "$pipes_json" | jq -c '.pipes[]' | while read -r pipe; do
        name=$(echo "$pipe" | jq -r '.name')
        
        if isInArray "$name" "${priorityPipes[@]}" || isInArray "$name" "${finalPipes[@]}"; then
            continue
        fi
        
        if should_process_pipe "$name" && ! isInArray "$name" "${pipeExclude[@]}"; then
            process_single_pipe "$name" "regular"
        fi
    done

    # Process final pipes last
    echo -e "\n${BOLD}⚙️  Processing final pipes${NC}\n"
    for name in "${finalPipes[@]}"; do
        if should_process_pipe "$name" && ! isInArray "$name" "${pipeExclude[@]}"; then
            process_single_pipe "$name" "final"
        else
            echo -e "${YELLOW}Skipping final pipe ${BOLD}$name${NC} (excluded or not matching criteria)${NC}"
        fi
    done
}

run_script() {    
    # Make sure user really wants to run the script
    if ! confirm; then
        echo -e "\n${YELLOW}Operation cancelled${NC}"
        return 1
    fi

    # Process data sources and pipes
    process_data_sources
    process_pipes

    echo -e "\n${GREEN}${BOLD}Processing complete!${NC}"
    return 0
}

main() {
    combine_exclusions

    case "$1" in
        dryrun)
            isDryRun=true
            if ! run_script; then
                return 1
            fi
            ;;
        repopulate)
            isDryRun=false
            if ! run_script; then
                return 1
            fi
            ;;
        *)
            print_usage
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
else
    # If the script is being sourced, don't exit as it would close the terminal window
    main "$@" || true
fi