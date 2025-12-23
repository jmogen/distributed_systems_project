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

  std::size_t total_elements = m * m;
  std::size_t elements_per_process = total_elements / process_group_size;
  std::size_t remaining_elements = total_elements % process_group_size;
  
  std::size_t local_elements = elements_per_process + (process_rank < remaining_elements ? 1 : 0);
  std::size_t local_rows = (local_elements + m - 1) / m;
  
  // init matrices - root reads, others get empty
  if (process_rank == 0) {
    output_matrix_c.resize(m * m, 0);
    read_matrix(m, m, input_matrix_a, input_filename_a);
    read_matrix(m, m, input_matrix_b, input_filename_b);
  } else {
    input_matrix_a.resize(m * m, 0);
    input_matrix_b.resize(m * m, 0);
  }
  
  MPI_Bcast(&m, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

  // Set up 2D grid for Cannon's algorithm
  int grid_size = static_cast<int>(std::sqrt(process_group_size));
  // if (grid_size * grid_size != process_group_size) {
  //   if (process_rank == 0) {
  //     std::cerr << "Number of processes must be a perfect square " << std::endl;
  //   }
  //   MPI_Abort(MPI_COMM_WORLD, 1);
  // }

  int dims[2] = {grid_size, grid_size};
  int periods[2] = {1, 1};
  MPI_Comm cart_comm;
  MPI_Cart_create(MPI_COMM_WORLD, 2, dims, periods, 1, &cart_comm);

  int coords[2];
  MPI_Cart_coords(cart_comm, process_rank, 2, coords);
  int row = coords[0], col = coords[1];

  // block size calc - align to 8 for better perf
  std::size_t block_size = (m + grid_size - 1) / grid_size;
  block_size = ((block_size + 7) / 8) * 8;
  
  // buffers for local computation
  std::vector<mentry_t> local_A(block_size * block_size, 0);
  std::vector<mentry_t> local_B(block_size * block_size, 0);
  std::vector<mentry_t> local_C(block_size * block_size, 0);
  std::vector<mentry_t> temp_A(block_size * block_size, 0);
  std::vector<mentry_t> temp_B(block_size * block_size, 0);
  std::vector<mentry_t> next_A(block_size * block_size, 0);
  std::vector<mentry_t> next_B(block_size * block_size, 0);

  // scatter matrices to processes
  if (process_rank == 0) {
    std::vector<mentry_t> sendbuf_A(process_group_size * block_size * block_size, 0);
    std::vector<mentry_t> sendbuf_B(process_group_size * block_size * block_size, 0);
    
    // prepare blocks for each process
    for (int pr = 0; pr < process_group_size; ++pr) {
      int pr_coords[2];
      MPI_Cart_coords(cart_comm, pr, 2, pr_coords);
      int brow = pr_coords[0], bcol = pr_coords[1];
      std::size_t offset = pr * block_size * block_size;
      
      for (std::size_t i = 0; i < block_size; ++i) {
        std::size_t row_offset = (brow * block_size + i) * m;
        std::size_t local_offset = i * block_size;
        for (std::size_t j = 0; j < block_size; ++j) {
          if (brow * block_size + i < m && bcol * block_size + j < m) {
            sendbuf_A[offset + local_offset + j] = input_matrix_a[row_offset + (bcol * block_size + j)];
            sendbuf_B[offset + local_offset + j] = input_matrix_b[row_offset + (bcol * block_size + j)];
          }
        }
      }
    }
    
    MPI_Request scatter_req_A, scatter_req_B;
    MPI_Iscatter(sendbuf_A.data(), block_size * block_size, MPI_UINT64_T,
                 local_A.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm, &scatter_req_A);
    MPI_Iscatter(sendbuf_B.data(), block_size * block_size, MPI_UINT64_T,
                 local_B.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm, &scatter_req_B);
    
    MPI_Wait(&scatter_req_A, MPI_STATUS_IGNORE);
    MPI_Wait(&scatter_req_B, MPI_STATUS_IGNORE);
  } else {
    MPI_Request scatter_req_A, scatter_req_B;
    MPI_Iscatter(nullptr, block_size * block_size, MPI_UINT64_T,
                 local_A.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm, &scatter_req_A);
    MPI_Iscatter(nullptr, block_size * block_size, MPI_UINT64_T,
                 local_B.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm, &scatter_req_B);
    
    MPI_Wait(&scatter_req_A, MPI_STATUS_IGNORE);
    MPI_Wait(&scatter_req_B, MPI_STATUS_IGNORE);
  }

  // initial alignment - shift blocks to correct positions
  for (int i = 0; i < row; ++i) {
    int left, right;
    MPI_Cart_shift(cart_comm, 1, -1, &right, &left);
    
    MPI_Request recv_req, send_req;
    std::copy(local_A.begin(), local_A.end(), temp_A.begin());
    MPI_Irecv(next_A.data(), block_size * block_size, MPI_UINT64_T, right, 0, cart_comm, &recv_req);
    MPI_Isend(temp_A.data(), block_size * block_size, MPI_UINT64_T, left, 0, cart_comm, &send_req);
    
    MPI_Wait(&recv_req, MPI_STATUS_IGNORE);
    MPI_Wait(&send_req, MPI_STATUS_IGNORE);
    std::copy(next_A.begin(), next_A.end(), local_A.begin());
  }

  for (int i = 0; i < col; ++i) {
    int up, down;
    MPI_Cart_shift(cart_comm, 0, -1, &down, &up);
    
    MPI_Request recv_req, send_req;
    std::copy(local_B.begin(), local_B.end(), temp_B.begin());
    MPI_Irecv(next_B.data(), block_size * block_size, MPI_UINT64_T, down, 0, cart_comm, &recv_req);
    MPI_Isend(temp_B.data(), block_size * block_size, MPI_UINT64_T, up, 0, cart_comm, &send_req);
    
    MPI_Wait(&recv_req, MPI_STATUS_IGNORE);
    MPI_Wait(&send_req, MPI_STATUS_IGNORE);
    std::copy(next_B.begin(), next_B.end(), local_B.begin());
  }

  // main computation loop
  for (int step = 0; step < grid_size; ++step) {
    if (step < grid_size - 1) {
      int left, right, up, down;
      MPI_Cart_shift(cart_comm, 1, -1, &right, &left);
      MPI_Cart_shift(cart_comm, 0, -1, &down, &up);
      
      MPI_Request recv_req_A, recv_req_B, send_req_A, send_req_B;
      MPI_Irecv(next_A.data(), block_size * block_size, MPI_UINT64_T, right, 0, cart_comm, &recv_req_A);
      MPI_Irecv(next_B.data(), block_size * block_size, MPI_UINT64_T, down, 0, cart_comm, &recv_req_B);
      
      // compute local block - try processing 2 at a time
      for (std::size_t i = 0; i < block_size; ++i) {
        for (std::size_t k = 0; k < block_size; k += 2) {
          if (k + 1 < block_size) {
            mentry_t a_ik0 = local_A[i * block_size + k];
            mentry_t a_ik1 = local_A[i * block_size + k + 1];
            
            for (std::size_t j = 0; j < block_size; ++j) {
              local_C[i * block_size + j] += 
                a_ik0 * local_B[k * block_size + j] +
                a_ik1 * local_B[(k + 1) * block_size + j];
            }
          } else {
            // handle remaining element
            mentry_t a_ik = local_A[i * block_size + k];
            for (std::size_t j = 0; j < block_size; ++j) {
              local_C[i * block_size + j] += a_ik * local_B[k * block_size + j];
            }
          }
        }
      }
      
      MPI_Isend(local_A.data(), block_size * block_size, MPI_UINT64_T, left, 0, cart_comm, &send_req_A);
      MPI_Isend(local_B.data(), block_size * block_size, MPI_UINT64_T, up, 0, cart_comm, &send_req_B);
      
      MPI_Wait(&recv_req_A, MPI_STATUS_IGNORE);
      MPI_Wait(&recv_req_B, MPI_STATUS_IGNORE);
      MPI_Wait(&send_req_A, MPI_STATUS_IGNORE);
      MPI_Wait(&send_req_B, MPI_STATUS_IGNORE);
      
      local_A.swap(next_A);
      local_B.swap(next_B);
    } else {
      // final computation step
      for (std::size_t i = 0; i < block_size; ++i) {
        for (std::size_t k = 0; k < block_size; k += 2) {
          if (k + 1 < block_size) {
            mentry_t a_ik0 = local_A[i * block_size + k];
            mentry_t a_ik1 = local_A[i * block_size + k + 1];
            
            for (std::size_t j = 0; j < block_size; ++j) {
              local_C[i * block_size + j] += 
                a_ik0 * local_B[k * block_size + j] +
                a_ik1 * local_B[(k + 1) * block_size + j];
            }
          } else {
            mentry_t a_ik = local_A[i * block_size + k];
            for (std::size_t j = 0; j < block_size; ++j) {
              local_C[i * block_size + j] += a_ik * local_B[k * block_size + j];
            }
          }
        }
      }
    }
  }

  // gather results back to root
  MPI_Request gather_req;
  if (process_rank == 0) {
    std::vector<mentry_t> gatherbuf(process_group_size * block_size * block_size, 0);
    MPI_Igather(local_C.data(), block_size * block_size, MPI_UINT64_T,
                gatherbuf.data(), block_size * block_size, MPI_UINT64_T, 0, cart_comm, &gather_req);
    
    MPI_Wait(&gather_req, MPI_STATUS_IGNORE);
    
    // put blocks in right places in output matrix
    for (int pr = 0; pr < process_group_size; ++pr) {
      int pr_coords[2];
      MPI_Cart_coords(cart_comm, pr, 2, pr_coords);
      int brow = pr_coords[0], bcol = pr_coords[1];
      std::size_t offset = pr * block_size * block_size;
      
      for (std::size_t i = 0; i < block_size; ++i) {
        if (brow * block_size + i < m) {
          std::size_t row_offset = (brow * block_size + i) * m;
          std::size_t local_offset = i * block_size;
          for (std::size_t j = 0; j < block_size; ++j) {
            if (bcol * block_size + j < m) {
              output_matrix_c[row_offset + (bcol * block_size + j)] = 
                gatherbuf[offset + local_offset + j];
            }
          }
        }
      }
    }
  } else {
    MPI_Igather(local_C.data(), block_size * block_size, MPI_UINT64_T,
                nullptr, block_size * block_size, MPI_UINT64_T, 0, cart_comm, &gather_req);
    MPI_Wait(&gather_req, MPI_STATUS_IGNORE);
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
