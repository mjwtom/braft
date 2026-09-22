#ifndef PUBLIC_RAFT_STACK_H
#define PUBLIC_RAFT_STACK_H

#include <elf.h>
#include <link.h>
#include <execinfo.h>
#include "base/debug/stack_trace.h"
#include "base/debug/proc_maps_linux.h"
#include "base/third_party/symbolize/symbolize.h"
#include "base/third_party/symbolize/demangle.h"
#include "base/memory/singleton.h"
#include "raft/util.h"

namespace raft {

DECLARE_int32(raft_max_stack_depth);

class StackSymbolizer {
public:
    struct SymInfo {
        ElfW(Ehdr) elf_header;
        std::map<uint64_t, std::pair<ElfW(Sym), base::IOBuf>> sym_map;
    };

    struct SymStat {
        uint64_t found_symbol_num = 0;
        uint64_t str_mem_size = 0;
    };

public: 
    static StackSymbolizer* GetInstance() {
        return Singleton<StackSymbolizer>::get();
    }

    bool init();

    bool symbolize(void *pc, char *out, int out_size);

private:
    StackSymbolizer() : _is_initialized(false) {}
    ~StackSymbolizer() {}
    DISALLOW_COPY_AND_ASSIGN(StackSymbolizer);
    friend struct DefaultSingletonTraits<StackSymbolizer>;
    bool load_sym_info(const int fd, SymInfo* sym_info, SymStat* stat, uint64_t region_start);
    bool load_sym_entry(int fd,
                        const ElfW(Shdr)& symtab,
                        SymInfo* sym_info,
                        SymStat* stat,
                        uint64_t region_start);
    bool get_section_header_by_type(const int fd,
                                    ElfW(Half) sh_num,
                                    const off_t sh_offset,
                                    ElfW(Word) type,
                                    ElfW(Shdr) *out);
    bool get_symbol(const SymInfo& sym_info,
                    uint64_t pc,
                    uint64_t symbol_address,
                    char *out,
                    int out_size);
    void DemangleInplace(char *out, int out_size);
    size_t find_first_0(const base::IOBuf* buf);

private:
    std::atomic<bool> _is_initialized;
    std::vector<base::debug::MappedMemoryRegion> _regions;
    std::map<std::string, SymInfo> _sym_info_map;
};

#define g_stack_symbolizer StackSymbolizer::GetInstance()

void init_stack_symbolizer_once();

std::string stack_string(uint32_t skip_nframes = 0, uint32_t max_depth = FLAGS_raft_max_stack_depth);

extern bvar::MultiDimension<bvar::WindowEx<BvarMaxer<uint64_t>, 30>> g_bthread_execute_max_latency_by_stack;

}  // namespace raft

#endif //~PUBLIC_RAFT_STACK_H
