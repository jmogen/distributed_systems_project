#include <cstdint>
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include <cmath>

#include <stdio.h>
#include <mpi.h>

using mentry_t = std::uint64_t;

// do not modify
void read_matrix(const std::size_t m, const std::size_t n, 
  std::vector<mentry_t>& matrix, const std::string filename) {

  std::ifstream file(filename, std::ifstream::in);  
  if (file.fail()) {
    std::cerr << "File error." << std::endl;
    return;
  }        
  
  std::string line;
  std::size_t line_count = 0; 
  while (std::getline(file, line) && line_count < m) {
    //std::cout << line << std::endl;
    std::istringstream ss(line);
    mentry_t e;    
    for (std::size_t i = 0; i < n; ++i) {
      ss >> e;
      matrix.emplace_back(e);
    }    
    line_count++;    
  }      
  file.close();
} // read_matrix

// do not modify
void write_matrix(const std::size_t m, const std::size_t n, 
  const std::vector<mentry_t>& matrix, const std::string filename) {

  std::ofstream file(filename, std::ofstream::out);
  if (file.fail()) {
    std::cerr << "File error." << std::endl;
    return;
  }

  std::size_t c = 0;
  for (auto e : matrix) {
    if (c == n - 1) {
      file << e << "\n";
      //std::cout << e << std::endl;
      c = 0;
    } else {          
      file << e << " ";
      //std::cout << e << " ";
      c++;
    }
  }  
  file.close();
} // write_matrix
  
// do not modify
void write_result(const std::vector<std::string>& result, 
  const std::string filename) {

  std::ofstream file(filename, std::ofstream::app); //std::ofstream::out);
  if (file.fail()) {
    std::cerr << "File error." << std::endl;
    return;
  }

  for (auto e : result) {
    file << e << ", ";
    std::cout << e << ", ";		  
  }
  file << "\n";   
  std::cout << std::endl;
  file.close(); 		
} // write_result
   
int main(int argc, char** argv) {

  int process_rank, process_group_size;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &process_group_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

  double start_time;
  double end_time;
  double elapsed_time;

  std::size_t m = std::stoul(argv[1]); //4; // #rows
  std::size_t n = m; // #columns

  std::string input_filename_a = argv[2]; //"matrix_a.txt";
  std::string input_filename_b = argv[3]; //"matrix_b.txt";
  std::string output_filename_c = argv[4]; //"matrix_c.txt";
  std::string output_filename_result = argv[5]; //"a4_result.txt";

  std::string input_experiment_name = argv[6]; // "d";

  // MPI collective operations require elements must be continuous in memory
  std::vector<mentry_t> input_matrix_a;
  std::vector<mentry_t> input_matrix_b;			 
  std::vector<mentry_t> output_matrix_c;

  std::vector<std::string> result;

  result.emplace_back(input_experiment_name);

  {
    std::stringstream ss;
	  ss << process_group_size;
	  result.emplace_back(ss.str());
  }

  {
    std::stringstream ss;		   
    ss << m;
    result.emplace_back(ss.str());
  }

  start_time = MPI_Wtime();

  // do not modify the code above  

  // your code begins //////////////////////////////////////////////////////////

  // Calculate work distribution
  std::size_t total_elements = m * m;  // Total elements in result matrix
  std::size_t elements_per_process = total_elements / process_group_size;
  std::size_t remaining_elements = total_elements % process_group_size;
  
  // Calculate local matrix sizes
  std::size_t local_elements = elements_per_process + (process_rank < remaining_elements ? 1 : 0);
  std::size_t local_rows = (local_elements + m - 1) / m;  // Ceiling division
  
  // Initialize matrices
  if (process_rank == 0) {
    output_matrix_c.resize(m * m, 0);  // Initialize output matrix
    read_matrix(m, m, input_matrix_a, input_filename_a);
    read_matrix(m, m, input_matrix_b, input_filename_b);
  } else {
    // Initialize empty matrices for non-root processes
    input_matrix_a.resize(m * m, 0);
    input_matrix_b.resize(m * m, 0);
  }
  
  // Ensure all processes have the correct size information
  MPI_Bcast(&m, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

  // Set up 2D grid for Cannon's algorithm
  int grid_size = static_cast<int>(std::sqrt(process_group_size));
  if (grid_size * grid_size != process_group_size) {
    if (process_rank == 0) {
      std::cerr << "Number of processes must be a perfect square for Cannon's algorithm" << std::endl;
    }
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  int dims[2] = {grid_size, grid_size};
  int periods[2] = {1, 1};  // Enable wraparound
  MPI_Comm cart_comm;
  MPI_Cart_create(MPI_COMM_WORLD, 2, dims, periods, 1, &cart_comm);

  int coords[2];
  MPI_Cart_coords(cart_comm, process_rank, 2, coords);
  int row = coords[0], col = coords[1];

  // Calculate block size
  std::size_t block_size = m / grid_size;
  
  // Pre-allocate all buffers to avoid reallocations
  std::vector<mentry_t> local_A(block_size * block_size, 0);
  std::vector<mentry_t> local_B(block_size * block_size, 0);
  std::vector<mentry_t> local_C(block_size * block_size, 0);
  std::vector<mentry_t> temp_A(block_size * block_size, 0);
  std::vector<mentry_t> temp_B(block_size * block_size, 0);
  std::vector<mentry_t> next_A(block_size * block_size, 0);
  std::vector<mentry_t> next_B(block_size * block_size, 0);

  // Optimize scatter operations by preparing data in a single pass
  if (process_rank == 0) {
    std::vector<mentry_t> sendbuf_A(process_group_size * block_size * block_size, 0);
    std::vector<mentry_t> sendbuf_B(process_group_size * block_size * block_size, 0);
    
    // Use a single loop for better cache utilization
    for (int pr = 0; pr < process_group_size; ++pr) {
      int pr_coords[2];
      MPI_Cart_coords(cart_comm, pr, 2, pr_coords);
      int brow = pr_coords[0], bcol = pr_coords[1];
      std::size_t offset = pr * block_size * block_size;
      
      for (std::size_t i = 0; i < block_size; ++i) {
        std::size_t row_offset = (brow * block_size + i) * m;
        std::size_t local_offset = i * block_size;
        for (std::size_t j = 0; j < block_size; ++j) {
          sendbuf_A[offset + local_offset + j] = input_matrix_a[row_offset + (bcol * block_size + j)];
          sendbuf_B[offset + local_offset + j] = input_matrix_b[row_offset + (bcol * block_size + j)];
        }
      }
    }
    
    MPI_Scatter(sendbuf_A.data(), block_size * block_size, MPI_UINT64_T,
                local_A.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm);
    MPI_Scatter(sendbuf_B.data(), block_size * block_size, MPI_UINT64_T,
                local_B.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm);
  } else {
    MPI_Scatter(nullptr, block_size * block_size, MPI_UINT64_T,
                local_A.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm);
    MPI_Scatter(nullptr, block_size * block_size, MPI_UINT64_T,
                local_B.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm);
  }

  // Initial alignment with optimized memory access
  for (int i = 0; i < row; ++i) {
    int left, right;
    MPI_Cart_shift(cart_comm, 1, -1, &right, &left);
    std::copy(local_A.begin(), local_A.end(), temp_A.begin());
    MPI_Sendrecv_replace(temp_A.data(), block_size * block_size, MPI_UINT64_T,
                        left, 0, right, 0, cart_comm, MPI_STATUS_IGNORE);
    std::copy(temp_A.begin(), temp_A.end(), local_A.begin());
  }

  for (int i = 0; i < col; ++i) {
    int up, down;
    MPI_Cart_shift(cart_comm, 0, -1, &down, &up);
    std::copy(local_B.begin(), local_B.end(), temp_B.begin());
    MPI_Sendrecv_replace(temp_B.data(), block_size * block_size, MPI_UINT64_T,
                        up, 0, down, 0, cart_comm, MPI_STATUS_IGNORE);
    std::copy(temp_B.begin(), temp_B.end(), local_B.begin());
  }

  // Main Cannon's algorithm loop with optimized computation
  for (int step = 0; step < grid_size; ++step) {
    // Optimized local block multiplication
    for (std::size_t i = 0; i < block_size; ++i) {
      for (std::size_t k = 0; k < block_size; ++k) {
        mentry_t a_ik = local_A[i * block_size + k];
        for (std::size_t j = 0; j < block_size; ++j) {
          local_C[i * block_size + j] += a_ik * local_B[k * block_size + j];
        }
      }
    }

    // Shift blocks if not the last step
    if (step < grid_size - 1) {
      int left, right, up, down;
      MPI_Cart_shift(cart_comm, 1, -1, &right, &left);
      MPI_Cart_shift(cart_comm, 0, -1, &down, &up);
      
      // Prepare next iteration's data while current computation is in progress
      std::copy(local_A.begin(), local_A.end(), next_A.begin());
      std::copy(local_B.begin(), local_B.end(), next_B.begin());
      
      MPI_Sendrecv_replace(next_A.data(), block_size * block_size, MPI_UINT64_T,
                          left, 0, right, 0, cart_comm, MPI_STATUS_IGNORE);
      MPI_Sendrecv_replace(next_B.data(), block_size * block_size, MPI_UINT64_T,
                          up, 0, down, 0, cart_comm, MPI_STATUS_IGNORE);
      
      // Swap buffers for next iteration
      local_A.swap(next_A);
      local_B.swap(next_B);
    }
  }

  // Optimized gather operation
  if (process_rank == 0) {
    std::vector<mentry_t> gatherbuf(process_group_size * block_size * block_size, 0);
    MPI_Gather(local_C.data(), block_size * block_size, MPI_UINT64_T,
               gatherbuf.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm);
    
    // Optimize output matrix construction
    for (int pr = 0; pr < process_group_size; ++pr) {
      int pr_coords[2];
      MPI_Cart_coords(cart_comm, pr, 2, pr_coords);
      int brow = pr_coords[0], bcol = pr_coords[1];
      std::size_t offset = pr * block_size * block_size;
      
      for (std::size_t i = 0; i < block_size; ++i) {
        std::size_t row_offset = (brow * block_size + i) * m;
        std::size_t local_offset = i * block_size;
        for (std::size_t j = 0; j < block_size; ++j) {
          output_matrix_c[row_offset + (bcol * block_size + j)] = 
            gatherbuf[offset + local_offset + j];
        }
      }
    }
  } else {
    MPI_Gather(local_C.data(), block_size * block_size, MPI_UINT64_T,
               nullptr, block_size * block_size, MPI_UINT64_T, 0, cart_comm);
  }

  // your code ends //////////////////////////////////////////////////////////// 

  // do not modify the code below

  MPI_Barrier(MPI_COMM_WORLD);

  end_time = MPI_Wtime(); // must be after the barrier
  elapsed_time = end_time - start_time;

  if (process_rank == 0) {
    std::cout << "Matrix multiplication computation time: " << elapsed_time << 
	  " seconds " << std::endl;
    {
      std::stringstream ss;
      ss << elapsed_time;
      result.emplace_back(ss.str());
    } 	
  }	

  MPI_Barrier(MPI_COMM_WORLD); 

  if (process_rank == 0) {
    double local_start_time =  MPI_Wtime(); 
    write_matrix(m, n, output_matrix_c, output_filename_c);  
	  double local_end_time =  MPI_Wtime();  
    double local_elapsed_time = local_end_time - local_start_time;
    std::cout << "MPI rank " << process_rank << " - write output time: " << 
      local_elapsed_time << " seconds " << std::endl; 	
  } 		   

  MPI_Barrier(MPI_COMM_WORLD);

  end_time = MPI_Wtime(); // must be after the barrier
  elapsed_time = end_time - start_time;

  if (process_rank == 0) {
    std::cout << "Matrix multiplication total time: " << elapsed_time << 
 	  " seconds " << std::endl;
    {
      std::stringstream ss;
      ss << elapsed_time;
      result.emplace_back(ss.str());
    }
    write_result(result, output_filename_result);	
  }  

  MPI_Finalize();
  return 0;
}
