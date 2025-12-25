#!/usr/bin/env python3
"""
Species Population Demographics Model
======================================
Realistic scientific workflow for testing Slurm escalation system.

Uses Leslie matrix models to simulate age-structured population dynamics
with stochastic demographic variation. Memory requirements scale with:
  pop_size × replicates × years × age_classes

Species-specific configurations (task_id % 10):
  0,5,9: Small populations (~100MB) - succeed at L0 (1GB)
  1,2:   Medium populations (~1.5GB) - fail L0, succeed L1 (2GB)
  3,7:   Long computation (timeout) - fail L0 (1min), succeed L1 (2min)
  4:     Large population (~3GB) - fail L0-L1, succeed L2 (4GB)
  6:     Very large population (~5GB) - fail L0-L2, succeed L3 (8GB)
  8:     Huge population (~7GB) - fail L0-L2, succeed L3 (8GB)
"""

import os
import sys
import time
import random
import math
from datetime import datetime

class SpeciesDemographics:
    """
    Age-structured population model using Leslie matrices.

    Simulates stochastic population dynamics with age-specific
    survival and fecundity rates.
    """

    def __init__(self, species_name, n_age_classes=10):
        self.species_name = species_name
        self.n_age_classes = n_age_classes
        self.leslie_matrix = None

    def build_leslie_matrix(self, survival_rates, fecundity_rates):
        """
        Construct Leslie matrix from vital rates.

        Args:
            survival_rates: List of age-specific survival probabilities
            fecundity_rates: List of age-specific fecundity values
        """
        n = self.n_age_classes
        L = [[0.0 for _ in range(n)] for _ in range(n)]

        # First row: fecundity
        for j in range(n):
            L[0][j] = fecundity_rates[j]

        # Subdiagonal: survival to next age class
        for i in range(n - 1):
            L[i + 1][i] = survival_rates[i]

        self.leslie_matrix = L
        return L

    def project_population(self, initial_pop, n_years, n_replicates,
                          demographic_stochasticity=True):
        """
        Project population forward with stochastic demographic variation.

        This is where memory is actually allocated and used.
        Memory: n_replicates × n_years × n_age_classes × 8 bytes

        Args:
            initial_pop: Initial population list (n_age_classes,)
            n_years: Number of years to project
            n_replicates: Number of stochastic replicates
            demographic_stochasticity: Add demographic variation

        Returns:
            trajectories: 3D list of shape (n_replicates, n_years, n_age_classes)
        """
        print(f"\nAllocating trajectory array: {n_replicates} replicates × {n_years} years × {self.n_age_classes} age classes")

        # Calculate expected memory
        memory_bytes = n_replicates * n_years * self.n_age_classes * 8
        memory_mb = memory_bytes / (1024 * 1024)
        print(f"Memory required: ~{memory_mb:.1f} MB")

        # Allocate the full array upfront (forces memory allocation)
        trajectories = [[[0.0 for _ in range(self.n_age_classes)] for _ in range(n_years)] for _ in range(n_replicates)]

        # Force the OS to actually allocate physical memory (not lazy allocation)
        # Write to every page (4KB = 512 float64 values)
        page_stride = 512
        total_size = n_replicates * n_years * self.n_age_classes
        for i in range(0, total_size, page_stride):
            rep_idx = i // (n_years * self.n_age_classes)
            year_idx = (i % (n_years * self.n_age_classes)) // self.n_age_classes
            age_idx = i % self.n_age_classes
            if rep_idx < n_replicates and year_idx < n_years:
                trajectories[rep_idx][year_idx][age_idx] = 0.0

        print(f"Physical memory allocated: {memory_mb:.1f} MB")

        # Run population projections
        print("\nRunning population projections...")
        for rep in range(n_replicates):
            if (rep + 1) % max(1, n_replicates // 10) == 0:
                print(f"  Completed {rep + 1}/{n_replicates} replicates...")

            # Initialize population
            pop = initial_pop[:]
            trajectories[rep][0] = pop[:]

            # Project forward
            for year in range(1, n_years):
                if demographic_stochasticity:
                    # Stochastic variation in vital rates (log-normal)
                    stochastic_matrix = [[self.leslie_matrix[i][j] * random.lognormvariate(0, 0.1)
                                         for j in range(self.n_age_classes)]
                                        for i in range(self.n_age_classes)]
                    pop = self._matrix_vector_mult(stochastic_matrix, pop)
                else:
                    pop = self._matrix_vector_mult(self.leslie_matrix, pop)

                # Add random environmental variation
                total_pop = sum(pop)
                pop = [max(0, pop[i] + random.gauss(0, 0.01 * total_pop)) for i in range(self.n_age_classes)]

                trajectories[rep][year] = pop[:]

        return trajectories

    def _matrix_vector_mult(self, matrix, vector):
        """Matrix-vector multiplication."""
        n = len(vector)
        result = [0.0] * n
        for i in range(n):
            for j in range(n):
                result[i] += matrix[i][j] * vector[j]
        return result

    def analyze_trajectories(self, trajectories):
        """
        Analyze population trajectories.

        Returns:
            extinction_prob: Probability of quasi-extinction
            mean_final: Mean final population size
            ci_lower, ci_upper: 95% confidence interval for final population
        """
        print("\nAnalyzing trajectories...")

        # Total population at each time point (sum over age classes)
        n_replicates = len(trajectories)
        n_years = len(trajectories[0])

        # Final population sizes
        final_pop = []
        for rep in range(n_replicates):
            total = sum(trajectories[rep][-1])  # Sum across age classes at final year
            final_pop.append(total)

        # Quasi-extinction threshold (< 10 individuals)
        extinct_count = sum(1 for pop in final_pop if pop < 10)
        extinction_prob = extinct_count / len(final_pop)

        # Summary statistics
        mean_final = sum(final_pop) / len(final_pop)
        sorted_pop = sorted(final_pop)
        ci_lower = sorted_pop[int(len(sorted_pop) * 0.025)]
        ci_upper = sorted_pop[int(len(sorted_pop) * 0.975)]

        return extinction_prob, mean_final, ci_lower, ci_upper


def get_species_config(species_id):
    """
    Get species configuration based on ID modulo 10.

    Returns deterministic memory and time requirements for testing.
    """
    configs = {
        0: {
            'name': 'Sparrow (small)',
            'pop_size': 1000,
            'replicates': 100,
            'years': 50,
            'runtime_sec': 5,
            'expected_mem_mb': 100,
            'expected_outcome': 'Completes at all levels'
        },
        1: {
            'name': 'Fox (medium)',
            'pop_size': 50000,
            'replicates': 3000,
            'years': 100,
            'runtime_sec': 10,
            'expected_mem_mb': 1536,
            'expected_outcome': 'OOM at L0 (1GB), completes at L1 (2GB)'
        },
        2: {
            'name': 'Deer (medium)',
            'pop_size': 60000,
            'replicates': 3200,
            'years': 100,
            'runtime_sec': 10,
            'expected_mem_mb': 1600,
            'expected_outcome': 'OOM at L0 (1GB), completes at L1 (2GB)'
        },
        3: {
            'name': 'Turtle (slow)',
            'pop_size': 1000,
            'replicates': 100,
            'years': 50,
            'runtime_sec': 90,
            'expected_mem_mb': 100,
            'expected_outcome': 'Timeout at L0 (1min), completes at L1 (2min)'
        },
        4: {
            'name': 'Bear (large)',
            'pop_size': 100000,
            'replicates': 4000,
            'years': 100,
            'runtime_sec': 10,
            'expected_mem_mb': 3072,
            'expected_outcome': 'OOM at L0-L1, completes at L2 (4GB)'
        },
        5: {
            'name': 'Robin (small)',
            'pop_size': 2000,
            'replicates': 200,
            'years': 50,
            'runtime_sec': 5,
            'expected_mem_mb': 200,
            'expected_outcome': 'Completes at all levels'
        },
        6: {
            'name': 'Elk (very large)',
            'pop_size': 150000,
            'replicates': 5000,
            'years': 100,
            'runtime_sec': 10,
            'expected_mem_mb': 5120,
            'expected_outcome': 'OOM at L0-L2, completes at L3 (8GB)'
        },
        7: {
            'name': 'Sloth (very slow)',
            'pop_size': 1000,
            'replicates': 100,
            'years': 50,
            'runtime_sec': 150,
            'expected_mem_mb': 100,
            'expected_outcome': 'Timeout at L0-L1, completes at L2 (4min)'
        },
        8: {
            'name': 'Bison (huge)',
            'pop_size': 180000,
            'replicates': 6000,
            'years': 100,
            'runtime_sec': 10,
            'expected_mem_mb': 7168,
            'expected_outcome': 'OOM at L0-L2, completes at L3 (8GB)'
        },
        9: {
            'name': 'Finch (small)',
            'pop_size': 1500,
            'replicates': 150,
            'years': 50,
            'runtime_sec': 5,
            'expected_mem_mb': 150,
            'expected_outcome': 'Completes at all levels'
        }
    }

    return configs[species_id % 10]


def main():
    """Main execution."""

    # Read Slurm environment variables
    task_id = int(os.environ.get('SLURM_ARRAY_TASK_ID', 0))
    mem_mb = os.environ.get('SLURM_MEM_PER_NODE', 'unknown')
    partition = os.environ.get('SLURM_JOB_PARTITION', 'unknown')
    level = os.environ.get('MEM_ESCALATE_LEVEL', '0')
    hostname = os.popen('hostname').read().strip()

    # Get species configuration
    config = get_species_config(task_id)

    # Print header
    print("=" * 60)
    print("Species Population Demographics Model")
    print("=" * 60)
    print(f"Species ID:     {task_id}")
    print(f"Memory:         {mem_mb}MB")
    print(f"Partition:      {partition}")
    print(f"Level:          {level}")
    print(f"Hostname:       {hostname}")
    print(f"Start:          {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print(f"Configuration:  {config['name']}")
    print(f"Population:     {config['pop_size']:,} individuals")
    print(f"Replicates:     {config['replicates']:,}")
    print(f"Projection:     {config['years']} years")
    print()
    print(f"Expected memory: ~{config['expected_mem_mb']} MB")
    print(f"Expected outcome: {config['expected_outcome']}")
    print()

    start_time = time.time()

    # Initialize demographic model
    model = SpeciesDemographics(config['name'], n_age_classes=10)

    # Set up vital rates (simplified - same for all species)
    survival_rates = [0.8, 0.75, 0.7, 0.7, 0.65, 0.6, 0.5, 0.4, 0.3, 0.0]
    fecundity_rates = [0.0, 0.5, 1.5, 2.0, 2.5, 2.0, 1.5, 1.0, 0.5, 0.0]

    # Build Leslie matrix
    print("Building Leslie matrix...")
    model.build_leslie_matrix(survival_rates, fecundity_rates)
    print("Leslie matrix constructed")

    # Initial population (stable age distribution)
    scale_factor = config['pop_size'] / 433
    initial_pop = [x * scale_factor for x in [100, 80, 64, 51, 41, 33, 26, 21, 17, 0]]

    # Run population projections (this is where memory is allocated)
    print("\nRunning population projections...")
    trajectories = model.project_population(
        initial_pop,
        config['years'],
        config['replicates'],
        demographic_stochasticity=True
    )

    # Simulate computational time if needed
    computation_time = time.time() - start_time
    if config['runtime_sec'] > computation_time:
        wait_time = config['runtime_sec'] - computation_time
        print(f"\nHolding computation for {wait_time:.1f}s to simulate analysis time...")
        for i in range(int(wait_time)):
            time.sleep(1)
            if (i + 1) % 10 == 0:
                print(f"  {i + 1}s elapsed...")

    # Analyze results
    extinction_prob, mean_final, ci_lower, ci_upper = model.analyze_trajectories(trajectories)

    # Print results
    total_time = time.time() - start_time
    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Extinction probability: {extinction_prob:.4f}")
    print(f"Final population (mean): {mean_final:.0f}")
    print(f"Final population (95% CI): [{ci_lower:.0f}, {ci_upper:.0f}]")
    print()
    print(f"Computation time: {total_time:.1f} seconds")
    print(f"End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Status: COMPLETED")
    print("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
