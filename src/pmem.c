/* pmem.c - Persistent Memory interface
 *
 * Copyright (c) 2020, Intel Corporation
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must start the above copyright notice,
 *     this quicklist of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this quicklist of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include "server.h"

#include <math.h>
#include <stdio.h>

#define THRESHOLD_STEP 0.05
#define THRESHOLD_UP(val)  ((size_t)ceil((1+THRESHOLD_STEP)*val))
#define THRESHOLD_DOWN(val) ((size_t)floor((1-THRESHOLD_STEP)*val))
//#define ABS_DIFF(a,b) a > b ? a - b : b -a

static inline size_t absDiff(size_t a, size_t b) {
    return a > b ? a - b : b -a;
}

/* Initialize the pmem threshold. */
void pmemThresholdInit(void)
{
    switch(server.memory_alloc_policy) {
        case MEM_POLICY_ONLY_DRAM:
            zmalloc_set_threshold(UINT_MAX);
            break;
        case MEM_POLICY_ONLY_PMEM:
            zmalloc_set_threshold(0U);
            break;
        case MEM_POLICY_THRESHOLD:
            zmalloc_set_threshold(server.static_threshold);
            break;
        case MEM_POLICY_RATIO:
            zmalloc_set_threshold(server.initial_dynamic_threshold);
            break;
        default:
            serverAssert(NULL);
    }
}
//TODO: modify logic to check the trend of actual Ratio using pmem_checkpoint_value and dram_checkpoint_value
void adjustPmemThresholdCycle(void) {
    if (server.memory_alloc_policy == MEM_POLICY_RATIO) {
        run_with_period(server.ratio_check_period) {
            size_t pmem_memory = zmalloc_used_pmem_memory();
            size_t dram_memory = zmalloc_used_memory();
            size_t total_memory = pmem_memory + dram_memory;
            size_t total_memory_checkpoint = server.pmem_checkpoint_value + server.dram_checkpoint_value;
            // do not modify threshold when change in memory usage is too small
            if(absDiff(total_memory_checkpoint, total_memory) > 100) {
                //revert logic to avoid division by zero
                double setting_state = (double)server.dram_pmem_ratio.pmem_val/server.dram_pmem_ratio.dram_val;
                double current_state = (double)pmem_memory/dram_memory;
                size_t threshold = zmalloc_get_threshold();
                if (fabs(setting_state-current_state) > 0.1) {
                    if (setting_state < current_state) {
                        size_t higher_threshold = THRESHOLD_UP(threshold);
                        if (higher_threshold > server.dynamic_threshold_max) return;
                        zmalloc_set_threshold(higher_threshold);
                    } else {
                        size_t lower_threshold = THRESHOLD_DOWN(threshold);
                        if (lower_threshold < server.dynamic_threshold_min) return;
                        zmalloc_set_threshold(lower_threshold);
                    }
                }
            }
            server.pmem_checkpoint_value = pmem_memory;
            server.dram_checkpoint_value = dram_memory;
        }
    }
}
