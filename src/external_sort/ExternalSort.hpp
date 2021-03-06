#ifndef __ExternalSort
#define __ExternalSort

#include "MemoryManager.hpp"
#include <list>
#include <memory>
#include <stdint.h>
#include <string>

namespace dbi {

class InputRun;
class OutputRun;

class ExternalSort {
public:
   /// Setup sport engine
   ExternalSort(const std::string& inputFileName, const std::string& outputFileName, uint64_t pageSize, uint64_t maxMemory, bool showPerformance);

   /// Start the sort
   void run();

private:
   const std::string inputFileName;
   const std::string outputFileName;
   const uint64_t availablePages;
   const bool showPerformance;
   MemoryManager buffer;

   /// Divide and conquer the algorithm !!!!
   std::list<std::unique_ptr<InputRun>> createRunsPhase();
   void mergeRunPhase(std::list<std::unique_ptr<InputRun>>& runs);
   void mergeSingleRun(std::list<std::unique_ptr<InputRun>>& inputRuns, uint32_t numJoins, OutputRun& targetRun);
};

}

#endif
