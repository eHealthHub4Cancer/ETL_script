# Set CRAN mirror for package installation
options(repos = c(CRAN = "https://cloud.r-project.org"))

# Function to install a package if it's not already installed
install_if_needed <- function(package_name) {
  if (!require(package_name, character.only = TRUE)) {
    install.packages(package_name, dependencies = TRUE)
  }
}

# Install devtools (for GitHub installations)
install_if_needed("devtools")
install_if_needed("rJava")

# Load devtools to ensure it's installed correctly
library(devtools)

# Install OHDSI-specific R packages
install_if_needed("DatabaseConnector")
install_if_needed("SqlRender")
devtools::install_github("OHDSI/CommonDataModel")  # For working with CDM-related functionality

# Install general-purpose R packages
packages <- c("lubridate", "dplyr", "readr", "arrow")
for (pkg in packages) {
  install_if_needed(pkg)
}

# Confirm installation
cat("\nInstalled R packages:\n")
print(installed.packages()[, "Package"])