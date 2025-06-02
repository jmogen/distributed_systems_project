#include <cstdint>
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>

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
git
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
  
  // Local matrices for each process
  std::vector<mentry_t> local_matrix_a(m * local_rows, 0);
  std::vector<mentry_t> local_matrix_c(local_elements, 0);
  
  // Calculate send counts and displacements for matrix A
  std::vector<int> sendcounts(process_group_size);
  std::vector<int> displs(process_group_size);
  
  int sum = 0;
  for (int i = 0; i < process_group_size; i++) {
    std::size_t proc_elements = elements_per_process + (i < remaining_elements ? 1 : 0);
    std::size_t proc_rows = (proc_elements + m - 1) / m;
    sendcounts[i] = proc_rows * m;
    displs[i] = sum;
    sum += sendcounts[i];
  }
  
  // Ensure all processes have the correct size information
  MPI_Bcast(&m, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
  
  // Scatter matrix A
  MPI_Scatterv(input_matrix_a.data(), sendcounts.data(), displs.data(), 
               MPI_UINT64_T, local_matrix_a.data(), m * local_rows, 
               MPI_UINT64_T, 0, MPI_COMM_WORLD);
  
  // Broadcast matrix B
  MPI_Bcast(input_matrix_b.data(), m * m, MPI_UINT64_T, 0, MPI_COMM_WORLD);
  
  // Calculate start and end positions for this process's portion of the result
  std::size_t start_pos = process_rank * elements_per_process + 
                         (process_rank < remaining_elements ? process_rank : remaining_elements);
  std::size_t end_pos = start_pos + local_elements;
  
  // Perform local matrix multiplication
  std::size_t current_pos = 0;
  for (std::size_t i = 0; i < local_rows && current_pos < local_elements; i++) {
    for (std::size_t j = 0; j < m && current_pos < local_elements; j++) {
      mentry_t sum = 0;
      for (std::size_t k = 0; k < m; k++) {
        sum += local_matrix_a[i * m + k] * input_matrix_b[k * m + j];
      }
      local_matrix_c[current_pos++] = sum;
    }
  }
  
  // Calculate receive counts and displacements for gathering results
  std::vector<int> recvcounts(process_group_size);
  std::vector<int> recvdispls(process_group_size);
  
  sum = 0;
  for (int i = 0; i < process_group_size; i++) {
    recvcounts[i] = elements_per_process + (i < remaining_elements ? 1 : 0);
    recvdispls[i] = sum;
    sum += recvcounts[i];
  }
  
  // Gather results back to process 0
  MPI_Gatherv(local_matrix_c.data(), local_elements, MPI_UINT64_T,
              output_matrix_c.data(), recvcounts.data(), recvdispls.data(),
              MPI_UINT64_T, 0, MPI_COMM_WORLD);

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
