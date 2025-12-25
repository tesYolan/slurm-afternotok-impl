#!/usr/bin/env Rscript
#
# Species Population Demographics Model (R Implementation)
# =========================================================
# Realistic scientific workflow for testing Slurm escalation system.
#
# Uses Leslie matrix models to simulate age-structured population dynamics
# with stochastic demographic variation. Memory requirements scale with:
#   pop_size × replicates × years × age_classes
#
# Species-specific configurations (task_id %% 10):
#   0,5,9: Small populations (~100MB) - succeed at L0 (1GB)
#   1,2:   Medium populations (~1.5GB) - fail L0, succeed L1 (2GB)
#   3,7:   Long computation (timeout) - fail L0 (1min), succeed L1 (2min)
#   4:     Large population (~3GB) - fail L0-L1, succeed L2 (4GB)
#   6:     Very large population (~5GB) - fail L0-L2, succeed L3 (8GB)
#   8:     Huge population (~7GB) - fail L0-L2, succeed L3 (8GB)

suppressMessages(library(methods))

# Species configuration function
get_species_config <- function(species_id) {
  configs <- list(
    list(
      name = "Sparrow (small)",
      pop_size = 1000,
      replicates = 100,
      years = 50,
      runtime_sec = 5,
      expected_mem_mb = 100,
      expected_outcome = "Completes at all levels"
    ),
    list(
      name = "Fox (medium)",
      pop_size = 50000,
      replicates = 3000,
      years = 100,
      runtime_sec = 10,
      expected_mem_mb = 1536,
      expected_outcome = "OOM at L0 (1GB), completes at L1 (2GB)"
    ),
    list(
      name = "Deer (medium)",
      pop_size = 60000,
      replicates = 3200,
      years = 100,
      runtime_sec = 10,
      expected_mem_mb = 1600,
      expected_outcome = "OOM at L0 (1GB), completes at L1 (2GB)"
    ),
    list(
      name = "Turtle (slow)",
      pop_size = 1000,
      replicates = 100,
      years = 50,
      runtime_sec = 90,
      expected_mem_mb = 100,
      expected_outcome = "Timeout at L0 (1min), completes at L1 (2min)"
    ),
    list(
      name = "Bear (large)",
      pop_size = 100000,
      replicates = 4000,
      years = 100,
      runtime_sec = 10,
      expected_mem_mb = 3072,
      expected_outcome = "OOM at L0-L1, completes at L2 (4GB)"
    ),
    list(
      name = "Robin (small)",
      pop_size = 2000,
      replicates = 200,
      years = 50,
      runtime_sec = 5,
      expected_mem_mb = 200,
      expected_outcome = "Completes at all levels"
    ),
    list(
      name = "Elk (very large)",
      pop_size = 150000,
      replicates = 5000,
      years = 100,
      runtime_sec = 10,
      expected_mem_mb = 5120,
      expected_outcome = "OOM at L0-L2, completes at L3 (8GB)"
    ),
    list(
      name = "Sloth (very slow)",
      pop_size = 1000,
      replicates = 100,
      years = 50,
      runtime_sec = 150,
      expected_mem_mb = 100,
      expected_outcome = "Timeout at L0-L1, completes at L2 (4min)"
    ),
    list(
      name = "Bison (huge)",
      pop_size = 180000,
      replicates = 6000,
      years = 100,
      runtime_sec = 10,
      expected_mem_mb = 7168,
      expected_outcome = "OOM at L0-L2, completes at L3 (8GB)"
    ),
    list(
      name = "Finch (small)",
      pop_size = 1500,
      replicates = 150,
      years = 50,
      runtime_sec = 5,
      expected_mem_mb = 150,
      expected_outcome = "Completes at all levels"
    )
  )

  # R uses 1-based indexing
  return(configs[[(species_id %% 10) + 1]])
}

# Build Leslie matrix
build_leslie_matrix <- function(survival_rates, fecundity_rates) {
  n <- length(survival_rates)
  L <- matrix(0, nrow = n, ncol = n)

  # First row: fecundity
  L[1, ] <- fecundity_rates

  # Subdiagonal: survival to next age class
  for (i in 1:(n - 1)) {
    L[i + 1, i] <- survival_rates[i]
  }

  return(L)
}

# Project population forward
project_population <- function(leslie_matrix, initial_pop, n_years,
                               n_replicates, demographic_stochasticity = TRUE) {
  n_age_classes <- length(initial_pop)

  cat(sprintf("\nAllocating trajectory array: %d replicates × %d years × %d age classes\n",
              n_replicates, n_years, n_age_classes))

  # Calculate expected memory
  memory_bytes <- n_replicates * n_years * n_age_classes * 8
  memory_mb <- memory_bytes / (1024 * 1024)
  cat(sprintf("Memory required: ~%.1f MB\n", memory_mb))

  # Allocate the full array upfront (forces memory allocation)
  trajectories <- array(0, dim = c(n_replicates, n_years, n_age_classes))

  # Force the OS to actually allocate physical memory (not lazy allocation)
  # Write to every page (4KB = 512 float64 values)
  page_stride <- 512
  flat_size <- n_replicates * n_years * n_age_classes
  for (i in seq(1, flat_size, by = page_stride)) {
    # Access memory to force allocation
    rep_idx <- ((i - 1) %/% (n_years * n_age_classes)) + 1
    year_idx <- (((i - 1) %% (n_years * n_age_classes)) %/% n_age_classes) + 1
    age_idx <- ((i - 1) %% n_age_classes) + 1
    if (rep_idx <= n_replicates && year_idx <= n_years && age_idx <= n_age_classes) {
      trajectories[rep_idx, year_idx, age_idx] <- 0.0
    }
  }

  cat(sprintf("Physical memory allocated: %.1f MB\n", memory_mb))

  # Run population projections
  cat("\nRunning population projections...\n")
  report_interval <- max(1, n_replicates %/% 10)

  for (rep in 1:n_replicates) {
    if (rep %% report_interval == 0) {
      cat(sprintf("  Completed %d/%d replicates...\n", rep, n_replicates))
    }

    # Initialize population
    pop <- initial_pop
    trajectories[rep, 1, ] <- pop

    # Project forward
    for (year in 2:n_years) {
      if (demographic_stochasticity) {
        # Stochastic variation in vital rates (log-normal)
        stochastic_matrix <- leslie_matrix * matrix(rlnorm(length(leslie_matrix), 0, 0.1),
                                                    nrow = nrow(leslie_matrix),
                                                    ncol = ncol(leslie_matrix))
        pop <- stochastic_matrix %*% pop
      } else {
        pop <- leslie_matrix %*% pop
      }

      # Add random environmental variation
      pop <- pmax(0, pop + rnorm(n_age_classes, 0, 0.01 * sum(pop)))

      trajectories[rep, year, ] <- as.vector(pop)
    }
  }

  return(trajectories)
}

# Analyze trajectories
analyze_trajectories <- function(trajectories) {
  cat("\nAnalyzing trajectories...\n")

  # Total population at each time point
  n_replicates <- dim(trajectories)[1]
  n_years <- dim(trajectories)[2]

  total_pop <- matrix(0, nrow = n_replicates, ncol = n_years)
  for (rep in 1:n_replicates) {
    for (year in 1:n_years) {
      total_pop[rep, year] <- sum(trajectories[rep, year, ])
    }
  }

  # Final population sizes
  final_pop <- total_pop[, n_years]

  # Quasi-extinction threshold (< 10 individuals)
  extinction_prob <- mean(final_pop < 10)

  # Summary statistics
  mean_final <- mean(final_pop)
  ci_lower <- quantile(final_pop, 0.025)
  ci_upper <- quantile(final_pop, 0.975)

  return(list(
    extinction_prob = extinction_prob,
    mean_final = mean_final,
    ci_lower = ci_lower,
    ci_upper = ci_upper
  ))
}

# Main execution
main <- function() {
  # Read Slurm environment variables
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID", "0"))
  mem_mb <- Sys.getenv("SLURM_MEM_PER_NODE", "unknown")
  partition <- Sys.getenv("SLURM_JOB_PARTITION", "unknown")
  level <- Sys.getenv("MEM_ESCALATE_LEVEL", "0")
  hostname <- system("hostname", intern = TRUE)

  # Get species configuration
  config <- get_species_config(task_id)

  # Print header
  cat(strrep("=", 60), "\n")
  cat("Species Population Demographics Model (R)\n")
  cat(strrep("=", 60), "\n")
  cat(sprintf("Species ID:     %d\n", task_id))
  cat(sprintf("Memory:         %sMB\n", mem_mb))
  cat(sprintf("Partition:      %s\n", partition))
  cat(sprintf("Level:          %s\n", level))
  cat(sprintf("Hostname:       %s\n", hostname))
  cat(sprintf("Start:          %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
  cat("\n")
  cat(sprintf("Configuration:  %s\n", config$name))
  cat(sprintf("Population:     %s individuals\n", format(config$pop_size, big.mark = ",")))
  cat(sprintf("Replicates:     %s\n", format(config$replicates, big.mark = ",")))
  cat(sprintf("Projection:     %d years\n", config$years))
  cat("\n")
  cat(sprintf("Expected memory: ~%d MB\n", config$expected_mem_mb))
  cat(sprintf("Expected outcome: %s\n", config$expected_outcome))
  cat("\n")

  start_time <- Sys.time()

  # Set up vital rates (simplified - same for all species)
  survival_rates <- c(0.8, 0.75, 0.7, 0.7, 0.65, 0.6, 0.5, 0.4, 0.3, 0.0)
  fecundity_rates <- c(0.0, 0.5, 1.5, 2.0, 2.5, 2.0, 1.5, 1.0, 0.5, 0.0)

  # Build Leslie matrix
  cat("Building Leslie matrix...\n")
  leslie_matrix <- build_leslie_matrix(survival_rates, fecundity_rates)
  cat("Leslie matrix constructed\n")

  # Initial population (stable age distribution)
  initial_pop <- c(100, 80, 64, 51, 41, 33, 26, 21, 17, 0) * (config$pop_size / 433)

  # Run population projections (this is where memory is allocated)
  cat("\nRunning population projections...\n")
  trajectories <- project_population(
    leslie_matrix,
    initial_pop,
    config$years,
    config$replicates,
    demographic_stochasticity = TRUE
  )

  # Simulate computational time if needed
  computation_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  if (config$runtime_sec > computation_time) {
    wait_time <- config$runtime_sec - computation_time
    cat(sprintf("\nHolding computation for %.1fs to simulate analysis time...\n", wait_time))
    for (i in 1:floor(wait_time)) {
      Sys.sleep(1)
      if (i %% 10 == 0) {
        cat(sprintf("  %ds elapsed...\n", i))
      }
    }
  }

  # Analyze results
  results <- analyze_trajectories(trajectories)

  # Print results
  total_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  cat("\n")
  cat(strrep("=", 60), "\n")
  cat("RESULTS\n")
  cat(strrep("=", 60), "\n")
  cat(sprintf("Extinction probability: %.4f\n", results$extinction_prob))
  cat(sprintf("Final population (mean): %.0f\n", results$mean_final))
  cat(sprintf("Final population (95%% CI): [%.0f, %.0f]\n", results$ci_lower, results$ci_upper))
  cat("\n")
  cat(sprintf("Computation time: %.1f seconds\n", total_time))
  cat(sprintf("End: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
  cat("Status: COMPLETED\n")
  cat(strrep("=", 60), "\n")

  return(0)
}

# Run main
if (!interactive()) {
  status <- main()
  quit(status = status)
}
