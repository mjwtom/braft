#include <gflags/gflags.h>
#include <baidu/hpn/rpc/reloadable_flags.h> 
#include "raft/stack.h"

namespace raft {
DEFINE_int32(raft_max_stack_depth, 3, "stack trace max depth");
HPN_RPC_VALIDATE_GFLAG(raft_max_stack_depth, baidu::hpn::rpc::PositiveInteger);

static BvarMaxer<uint64_t> g_stack_string_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_stack_string_max_latency_in_window(
    "stack_string_max_latency_in_window", &g_stack_string_max_latency, 30);
static BvarAdder<uint64_t> g_stack_string_count;
static bvar::PerSecond<BvarAdder<uint64_t>> g_stack_string_count_second(
    "stack_string_count_second", &g_stack_string_count);

bool StackSymbolizer::get_symbol(const SymInfo& sym_info,
                                 uint64_t pc,
                                 uint64_t symbol_address,
                                 char *out,
                                 int out_size) {
    auto it = sym_info.sym_map.upper_bound(pc);
    if (it == sym_info.sym_map.end() || it == sym_info.sym_map.begin()) {
        return false;
    }
    it--;
    uint64_t key = it->first;
    const ElfW(Sym)& symbol = it->second.first;
    const base::IOBuf& buf = it->second.second;
    uint64_t start_address = symbol_address + symbol.st_value;
    CHECK_EQ(key, start_address);
    uint64_t end_address = start_address + symbol.st_size;
    if (start_address <= pc && pc < end_address) {
        buf.copy_to_cstr(out, out_size - 1);
        return true;
    }
    return false;
}

void StackSymbolizer::DemangleInplace(char *out, int out_size) {
    char demangled[256];  // Big enough for sane demangled symbols.
    if (google::Demangle(out, demangled, sizeof(demangled))) {
        // Demangling succeeded. Copy to out if the space allows.
        size_t len = strlen(demangled);
        if (len + 1 <= (size_t)out_size) {  // +1 for '\0'.
          CHECK(len < sizeof(demangled));
          memmove(out, demangled, len + 1);
        }
    }
}

bool StackSymbolizer::symbolize(void *pc, char *out, int out_size) {
    if (!_is_initialized.load()) {
        LOG(WARNING) << "StackSymbolizer not initialized yet";
        return false;
    }
    CHECK_GE(out_size, 0);
    uint64_t pc0 = reinterpret_cast<uintptr_t>(pc);
    std::vector<base::debug::MappedMemoryRegion>::const_iterator it;
    for (it = _regions.begin(); it != _regions.end(); ++it) {
        const base::debug::MappedMemoryRegion& region = *it;
        if (region.start <= pc0 && pc0 < region.end) {
            auto it = _sym_info_map.find(region.path);
            if (it == _sym_info_map.end()) {
                return false;
            }
            const SymInfo& sym_info = it->second;
            uint64_t symbol_offset = 0;
            if (sym_info.elf_header.e_type == ET_DYN) {  // DSO needs offset adjustment.
                symbol_offset = region.start;
            }
            if (!get_symbol(sym_info, pc0, symbol_offset, out, out_size)) {
                return false;
            }
            DemangleInplace(out, out_size);
            return true;
        }
    }
    return false;
}

size_t StackSymbolizer::find_first_0(const base::IOBuf* buf) {
    size_t len = 0;
    size_t block_num = buf->backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        base::StringPiece str = buf->backing_block(i);
        size_t pos = str.find_first_of('\0');
        if (pos != base::StringPiece::npos) {
            len += pos;
            break;
        }
        len += str.length();
    }
    return len;
}

bool StackSymbolizer::load_sym_entry(int fd,
                                     const ElfW(Shdr)& symtab,
                                     SymInfo* sym_info,
                                     SymStat* stat,
                                     uint64_t region_start) {
    base::IOPortal portal;
    ElfW(Shdr) strtab;
    off_t offset = sym_info->elf_header.e_shoff + symtab.sh_link * sizeof(symtab);
    ssize_t len = file_pread(&portal, fd, offset, sizeof(strtab));
    CHECK(len == sizeof(strtab));
    portal.cutn(&strtab, portal.length());

    size_t batch_num = 32;
    const int num_symbols = symtab.sh_size / symtab.sh_entsize;
    stat->found_symbol_num += num_symbols;
    for (int i = 0; i < num_symbols;) {
        base::IOPortal portal_for_sym;
        offset = symtab.sh_offset + i * symtab.sh_entsize;
        ssize_t len = file_pread(&portal_for_sym, fd, offset, batch_num * sizeof(ElfW(Sym)));
        CHECK(len % sizeof(ElfW(Sym)) == 0) << "len: " << len;
        const ssize_t num_symbols_in_buf = len / sizeof(ElfW(Sym));
        CHECK(num_symbols_in_buf <= batch_num);
        for (int j = 0; j < num_symbols_in_buf; ++j) {
            ElfW(Sym) symbol;
            portal_for_sym.cutn(&symbol, sizeof(symbol));
            if (symbol.st_value == 0  // Skip null value symbols.
                || symbol.st_shndx == 0) {  // Skip undefined symbols.
                continue;
            }
            offset = strtab.sh_offset + symbol.st_name;
            base::IOPortal portal_for_str;
            len = file_pread(&portal_for_str, fd, offset, 256);
            size_t pos = find_first_0(&portal_for_str);
            CHECK_GE(len, 0);
            if (len == 0 || pos == portal_for_str.length()) {
                continue;
            }
            base::IOBuf buf;
            portal_for_str.cutn(&buf, pos);
            stat->str_mem_size += buf.size();
            uint64_t start_address = symbol.st_value + region_start;
            auto it = sym_info->sym_map.find(start_address);
            if (it == sym_info->sym_map.end()) {
                sym_info->sym_map.emplace(start_address, std::make_pair(std::move(symbol), std::move(buf)));
            }
        }
        CHECK(portal_for_sym.empty());
        i += num_symbols_in_buf;
    }
    return true;
}

bool StackSymbolizer::get_section_header_by_type(const int fd,
                                                 ElfW(Half) sh_num,
                                                 const off_t sh_offset,
                                                 ElfW(Word) type,
                                                 ElfW(Shdr) *out) {
  size_t batch_num = 16;
  for (int i = 0; i < sh_num;) {
      const ssize_t num_bytes_left = (sh_num - i) * sizeof(ElfW(Shdr));
      const ssize_t num_bytes_to_read
          = (batch_num * sizeof(ElfW(Shdr)) > num_bytes_left) ? num_bytes_left : sizeof(ElfW(Shdr));
      base::IOPortal portal;
      off_t offset = sh_offset + i * sizeof(ElfW(Shdr));
      const ssize_t len = file_pread(&portal, fd, offset, num_bytes_to_read);
      CHECK(len % sizeof(ElfW(Shdr)) == 0) << "len: " << len;
      const ssize_t num_headers_in_buf = len / sizeof(ElfW(Shdr));
      CHECK(num_headers_in_buf <= batch_num);
      for (int j = 0; j < num_headers_in_buf; ++j) {
          ElfW(Shdr) shdr;
          portal.cutn(&shdr, sizeof(shdr));
          if (shdr.sh_type == type) {
            *out = shdr;
            return true;
          }
      }
      CHECK(portal.empty());
      i += num_headers_in_buf;
  }
  return false;
}

bool StackSymbolizer::load_sym_info(const int fd, SymInfo* sym_info, SymStat* stat, uint64_t region_start) {
    base::IOPortal portal;
    ssize_t len = file_pread(&portal, fd, 0, sizeof(sym_info->elf_header));
    CHECK(len == sizeof(sym_info->elf_header));
    portal.cutn(&sym_info->elf_header, portal.length());

    ElfW(Shdr) symtab;
    if (get_section_header_by_type(fd,
                                   sym_info->elf_header.e_shnum,
                                   sym_info->elf_header.e_shoff,
                                   SHT_SYMTAB,
                                   &symtab)) {
        load_sym_entry(fd, symtab, sym_info, stat, 0);
        return true;
    }
    if (get_section_header_by_type(fd,
                                   sym_info->elf_header.e_shnum,
                                   sym_info->elf_header.e_shoff,
                                   SHT_DYNSYM,
                                   &symtab)) {
        load_sym_entry(fd, symtab, sym_info, stat, region_start);
        return true;
    }
    return false;
}

bool StackSymbolizer::init() {
    std::string contents;
    if (!base::debug::ReadProcMaps(&contents)) {
        LOG(ERROR) << "Failed to read /proc/self/maps";
        return false;
    }
    if (!ParseProcMaps(contents, &_regions)) {
        LOG(ERROR) << "Failed to parse the contents of /proc/self/maps";
        return false;
    }
    uint64_t total_mem_size = 0;
    std::vector<base::debug::MappedMemoryRegion>::const_iterator it;
    for (it = _regions.begin(); it != _regions.end(); ++it) {
        const base::debug::MappedMemoryRegion& region = *it;
        // Only interesed in read-only executable regions.
        if ((region.permissions & base::debug::MappedMemoryRegion::READ) ==
                base::debug::MappedMemoryRegion::READ &&
            (region.permissions & base::debug::MappedMemoryRegion::WRITE) == 0 &&
            (region.permissions & base::debug::MappedMemoryRegion::EXECUTE) ==
                base::debug::MappedMemoryRegion::EXECUTE) {
            if (region.path.empty()) {
                // Skip regions with empty file names.
                continue;
            }
            if (region.path[0] == '[') {
                // Skip pseudo-paths, like [stack], [vdso], [heap], etc ...
                continue;
            }
            // Avoid duplicates.
            if (_sym_info_map.find(region.path) == _sym_info_map.end()) {
                int fd = ::open(region.path.c_str(), O_RDONLY | O_CLOEXEC);
                if (fd < 0) {
                    LOG(ERROR) << "Failed to open file: " << region.path
                               << " for loading symbles, error: " << strerror(errno);
                    return false;
                }
                SymInfo sym_info;
                SymStat stat;
                load_sym_info(fd, &sym_info, &stat, region.start);
                ::close(fd);
                uint64_t used_mem_size = sym_info.sym_map.size()
                    * (sizeof(ElfW(Sym)) + sizeof(uint64_t) + stat.str_mem_size);
                LOG(NOTICE) << "found " << stat.found_symbol_num << " symbols"
                            << ", cached " << sym_info.sym_map.size() << " symbols"
                            << ", used mem_size " << used_mem_size
                            << " from " << region.path;
                total_mem_size += used_mem_size;
                _sym_info_map[region.path] = std::move(sym_info);
            }
        }
    }
    LOG(NOTICE) << "cached symbols used"
                << " total_mem_size " << total_mem_size;
    _is_initialized.store(true);
    return true;
}

std::string stack_string(uint32_t skip_nframes, uint32_t max_depth) {
    g_stack_string_count << 1;
    uint64_t start_time_us = base::monotonic_time_us();
    std::string out_str;
    void* stack[62];
    int nframes = ::backtrace(stack, 62);
#ifdef NDEBUG
    int start_frame = 1;
#else
    int start_frame = 2;
#endif
    start_frame += skip_nframes;
    if (nframes < start_frame + 1) {
        return "noframe";
    }
    int end_frame = start_frame + max_depth;
    end_frame = std::min(end_frame, nframes);
    CHECK_LE(start_frame, end_frame);
    for (int i = start_frame; i < end_frame; ++i) {
        char frame_buf[1024] = { '\0' };
        if ( i > start_frame) {
            out_str.append("<-");
        }
        void* frame = static_cast<char*>(stack[i]) - 1;
        if (g_stack_symbolizer->symbolize(frame, frame_buf, sizeof(frame_buf))) {
            out_str.append(frame_buf);
        } else {
            out_str.append("unknown");
        }
    }
    g_stack_string_max_latency << base::monotonic_time_us() - start_time_us;
    return out_str;
}

static void init_stack_symbolizer() {
    g_stack_symbolizer->init();
}

static pthread_once_t s_init_once = PTHREAD_ONCE_INIT;

void init_stack_symbolizer_once() {
    CHECK_EQ(0, pthread_once(&s_init_once, init_stack_symbolizer));
}

}  // namespace raft
