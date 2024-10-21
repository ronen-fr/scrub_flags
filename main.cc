#include <benchmark/benchmark.h>
#include <fmt/core.h>

#include <cassert>
#include <chrono>
#include <compare>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>


#define RUN_THE_BM

using namespace std;

struct Target {
  int urgency;
  int level;
  string msg;

  // data used to slow down the comparisons:
  struct Schedule {
    chrono::steady_clock::time_point not_before;
    chrono::steady_clock::time_point scheduled_at;
  } schedule;

  Target(int u, int lv, string_view m) : urgency{u}, level{lv}, msg{m}
  {
    int rnd = rand() % 100'000;
    schedule.not_before = chrono::steady_clock::now() - chrono::seconds(rnd);
    schedule.scheduled_at = chrono::steady_clock::now() - 2 * chrono::seconds(rnd);
  }
  bool is_high_priority() const { return true; }
  bool is_deep() const { return level; }
  std::string urgency_txt() const { return fmt::format("U({})", urgency); }
};


struct Job {
  Job(bool b, string nmp) : blocked{b}, nm{nmp} {}

  bool blocked;
  string nm;
  Target a{true, false, nm};
  Target b{false, true, nm};

  Target& earliest_target();
  const Target& earliest_target() const;
  Target& earliest_target(chrono::steady_clock::time_point scrub_clock_now);
  const Target& earliest_target(chrono::steady_clock::time_point scrub_clock_now) const;
};

static inline std::weak_ordering cmp_ripe_entries(const Target& l,
                                                  const Target& r) noexcept
{
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  // if we are comparing the two targets of the same PG, once both are
  // ripe - the 'deep' scrub is considered 'higher' than the 'shallow' one.
//   if (l.pgid == r.pgid && r.level < l.level) {
//     return std::weak_ordering::less;
//   }
  // the 'chrono::steady_clock::time_point' operator<=> is 'partial_ordering', it seems.
  if (auto cmp = std::weak_order(l.schedule.scheduled_at, r.schedule.scheduled_at);
      cmp != 0) {
    return cmp;
  }
  if (r.level < l.level) {
    return std::weak_ordering::less;
  }
  if (auto cmp = std::weak_order(l.schedule.not_before, r.schedule.not_before);
      cmp != 0) {
    return cmp;
  }
  return std::weak_ordering::greater;
}

static inline std::weak_ordering cmp_future_entries(const Target& l,
                                                    const Target& r) noexcept
{
  if (auto cmp =
        // std::weak_order(double(l.schedule.not_before), double(r.schedule.not_before));
      std::weak_order(l.schedule.not_before, r.schedule.not_before);
      cmp != 0) {
    return cmp;
  }
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  if (auto cmp = std::weak_order(l.schedule.scheduled_at, r.schedule.scheduled_at);
      cmp != 0) {
    return cmp;
  }
  if (r.level < l.level) {
    return std::weak_ordering::less;
  }
  return std::weak_ordering::greater;
}

static inline std::weak_ordering cmp_entries(chrono::steady_clock::time_point t,
                                             const Target& l,
                                             const Target& r) noexcept
{
  bool l_ripe = l.schedule.not_before <= t;
  bool r_ripe = r.schedule.not_before <= t;
  if (l_ripe) {
    if (r_ripe) {
      return cmp_ripe_entries(l, r);
    }
    return std::weak_ordering::less;
  }
  if (r_ripe) {
    return std::weak_ordering::greater;
  }
  return cmp_future_entries(l, r);
}



Target& Job::earliest_target()
{
  std::weak_ordering compr = cmp_future_entries(a, b);
  return (compr == std::weak_ordering::less) ? a : b;
}

const Target& Job::earliest_target() const
{
  std::weak_ordering compr = cmp_future_entries(a, b);
  return (compr == std::weak_ordering::less) ? a : b;
}


Target& Job::earliest_target(chrono::steady_clock::time_point scrub_clock_now)
{
  std::weak_ordering compr = cmp_entries(scrub_clock_now, a, b);
  return (compr == std::weak_ordering::less) ? a : b;
}

const Target& Job::earliest_target(chrono::steady_clock::time_point scrub_clock_now) const
{
  std::weak_ordering compr = cmp_entries(scrub_clock_now, a, b);
  return (compr == std::weak_ordering::less) ? a : b;
}


//   const Target& earliest_target(const chrono::steady_clock::time_point& now) const
//   {
//     if (now.time_since_epoch().count() % 2) {
//       return a;
//     }
//     return b;
//   }




struct scrub_flags_t {

  unsigned int priority{0};

  /**
   * set by set_op_parameters() for deep scrubs, if the hardware
   * supports auto repairing and osd_scrub_auto_repair is enabled.
   */
  bool auto_repair{false};

  /// this flag indicates that we are scrubbing post repair to verify everything
  /// is fixed (otherwise - PG_STATE_FAILED_REPAIR will be asserted.)
  /// Update (July 2024): now reflects an 'after-repair' urgency.
  bool check_repair{false};

  /// checked at the end of the scrub, to possibly initiate a deep-scrub
  bool deep_scrub_on_error{false};
};

// ostream& operator<<(ostream& out, const scrub_flags_t& sf);

namespace fmt {
template <> struct formatter<scrub_flags_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const scrub_flags_t& sf, FormatContext& ctx) const
  {
    std::string txt;
    bool sep{false};
    if (sf.auto_repair) {
      txt = "auto-repair";
      sep = true;
    }
    if (sf.check_repair) {
      txt += sep ? ",check-repair" : "check-repair";
      sep = true;
    }
    if (sf.deep_scrub_on_error) {
      txt += sep ? ",deep-scrub-on-error" : "deep-scrub-on-error";
      sep = true;
    }
    return fmt::format_to(ctx.out(), "{}", txt);
  }
};
}  // namespace fmt



struct PgScrubber {
  PgScrubber(bool set_mact, bool fg_a, bool fg_b, bool fg_c, bool fg_d)
      : m_scrub_flags{fg_a, fg_b, fg_c, fg_d}
  {
    m_scrub_job = new Job{false, "job"};
    cached_flags = fmt::format("{}", m_scrub_flags);
    if (set_mact) {
      m_active_target = m_scrub_job->earliest_target(chrono::steady_clock::now());
    }
  }

  bool is_primary() const { return true; }
  ostream& show_concise(ostream& out) const;
  ostream& show_2(ostream& out) const;

  scrub_flags_t m_scrub_flags;

  bool m_active = true;  // must be true for this test
  std::optional<Target> m_active_target;

  string cached_flags;

  std::string get_cur_scrub_flags_text() const
  {
    return fmt::format("{}", m_scrub_flags);
  }

  std::string get_cur_scrub_flags_text2() const { return cached_flags; }

  Job* m_scrub_job;
};



ostream& PgScrubber::show_concise(ostream& out) const
{
  /*
   * 'show_concise()' is only used when calling operator<< thru the ScrubPgIF,
   * i.e. only by the PG when creating a standard log entry.
   *
   * desired outcome (only relevant for Primaries):
   *
   * if scrubbing:
   *   (urgency flags)
   *   or (if blocked)
   *   (*blocked*,urgency flags)
   *
   * if not scrubbing:
   *   either nothing (if only periodic scrubs are scheduled)
   *   or [next scrub: effective-lvl, urgency, rpr,
   */
  if (!is_primary()) {
    return out;
  }

  if (m_active) {
    const auto flags_txt = get_cur_scrub_flags_text();
    const std::string sep = (flags_txt.empty() ? "" : ",");
    if (m_active_target) {
      return out << fmt::format("({}{}{}{})", (m_scrub_job->blocked ? "*blocked*," : ""),
                                m_active_target->urgency_txt(), sep, flags_txt);
    } else {
      return out << fmt::format("(in-act{}{}{})",
                                (m_scrub_job->blocked ? "-*blocked*" : ""), sep,
                                flags_txt);
    }
  }

  // not actively scrubbing now. Show some info about the next scrub
  const auto now_is = chrono::steady_clock::now();
  const auto& next_scrub = m_scrub_job->earliest_target(now_is);
  if (!next_scrub.is_high_priority()) {
    // no interesting flags to report
    return out;
  }
  return out << fmt::format("[next-scrub:{},{:10.10}]",
                            (next_scrub.is_deep() ? "dp" : "sh"),
                            next_scrub.urgency_txt());
}


ostream& PgScrubber::show_2(ostream& out) const
{
  /*
   * 'show_concise()' is only used when calling operator<< thru the ScrubPgIF,
   * i.e. only by the PG when creating a standard log entry.
   *
   * desired outcome (only relevant for Primaries):
   *
   * if scrubbing:
   *   (urgency flags)
   *   or (if blocked)
   *   (*blocked*,urgency flags)
   *
   * if not scrubbing:
   *   either nothing (if only periodic scrubs are scheduled)
   *   or [next scrub: effective-lvl, urgency, rpr,
   */
  if (!is_primary()) {
    return out;
  }

  if (m_active) {
    const auto flags_txt = get_cur_scrub_flags_text2();
    const std::string sep = (flags_txt.empty() ? "" : ",");
    if (m_active_target) {
      return out << fmt::format("({}{}{}{})", (m_scrub_job->blocked ? "*blocked*," : ""),
                                m_active_target->urgency_txt(), sep, flags_txt);
    } else {
      return out << fmt::format("(in-act{}{}{})",
                                (m_scrub_job->blocked ? "-*blocked*" : ""), sep,
                                flags_txt);
    }
  }

  // not actively scrubbing now. Show some info about the next scrub
  const auto now_is = chrono::steady_clock::now();
  const auto& next_scrub = m_scrub_job->earliest_target(now_is);
  if (!next_scrub.is_high_priority()) {
    // no interesting flags to report
    return out;
  }
  return out << fmt::format("[next-scrub:{},{:10.10}]",
                            (next_scrub.is_deep() ? "dp" : "sh"),
                            next_scrub.urgency_txt());
}



// //////////////////////////////////////////////////////////////////////////////////////


void BM_no_m_act_no_cache_f(benchmark::State& state)
{
  PgScrubber pg_wo_flags{false, false, false, false, false};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg_wo_flags.show_concise(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_no_m_act_no_cache_f);

void BM_no_m_act_no_cache_t(benchmark::State& state)
{
  PgScrubber pg_w_flags{false, true, true, true, true};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg_w_flags.show_concise(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_no_m_act_no_cache_t);


// //////////////////////////////////////////////////////////////////////////////////////


void BM_no_m_act_cache_f(benchmark::State& state)
{
  PgScrubber pg_wo_flags{false, false, false, false, false};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg_wo_flags.show_2(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_no_m_act_cache_f);

void BM_no_m_act_cache_t(benchmark::State& state)
{
  PgScrubber pg_w_flags{false, true, true, true, true};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg_w_flags.show_2(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_no_m_act_cache_t);

// //////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////////

// //////////////////////////////////////////////////////////////////////////////////////


void BM_m_act_no_cache_f(benchmark::State& state)
{
  PgScrubber pg_wo_flags{true, false, false, false, false};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg_wo_flags.show_concise(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_m_act_no_cache_f);

void BM_m_act_no_cache_t(benchmark::State& state)
{
  PgScrubber pg_w_flags{true, true, true, true, true};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg_w_flags.show_concise(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_m_act_no_cache_t);


// //////////////////////////////////////////////////////////////////////////////////////


void BM_m_act_cache_f(benchmark::State& state)
{
  PgScrubber pg_wo_flags{true, false, false, false, false};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg_wo_flags.show_2(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_m_act_cache_f);

void BM_m_act_cache_t(benchmark::State& state)
{
  PgScrubber pg_w_flags{true, true, true, true, true};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg_w_flags.show_2(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_m_act_cache_t);

// //////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////////

void BM_no_cache_false(benchmark::State& state)
{
  PgScrubber pg1{false, false, false, false, false};
  PgScrubber pg2{false, false, false, false, false};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg1.show_concise(os);
    pg2.show_concise(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_no_cache_false);

void BM_no_cache_true(benchmark::State& state)
{
  PgScrubber pg1{true, true, true, true, true};
  PgScrubber pg2{true, true, true, true, true};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg1.show_concise(os);
    pg2.show_concise(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_no_cache_true);


void BM_cached_false(benchmark::State& state)
{
  PgScrubber pg1{false, false, false, false, false};
  PgScrubber pg2{false, false, false, false, false};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg1.show_2(os);
    pg2.show_2(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_cached_false);


void BM_cached_true(benchmark::State& state)
{
  PgScrubber pg1{true, true, true, true, true};
  PgScrubber pg2{true, true, true, true, true};

  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    ostringstream os;
    pg1.show_2(os);
    pg2.show_2(os);
    rss = os.str().size();
  }
}
BENCHMARK(BM_cached_true);

#ifndef RUN_THE_BM
int main()
{
  PgScrubber pg1{true, true, true, true, true};
  PgScrubber pg2{false, false, false, false, false};

  {
    ostringstream os;
    pg1.show_concise(os);
    cout << "pg1: " << os.str() << endl;
  }
  {
    ostringstream os;
    pg2.show_concise(os);
    cout << "pg2: " << os.str() << endl;
  }
}
#endif


#if 0

CephContext stam;


// initial sanity check for both implementations
void exp_orig()
{
  MemoryModel mm{&stam};
  MemoryModel::snap s;
  mm.sample(&s);

  auto heap1 = mm.compute_heap();

  [[maybe_unused]] volatile char* p = new char[24 * 1024 * 1024];
  MemoryModel::snap t;
  mm.sample(&t);
  delete[] p;
  auto heap2 = mm.compute_heap();

  fmt::print("{}:\t{}\n\t\t{}\n\theap: {} {}\n", __func__, s, t, heap1, heap2);
}


void exp_new()
{
  MM2 mm;
  auto s = mm.sample();
  auto heap1 = mm.compute_heap();
  [[maybe_unused]] volatile char* p = new char[36 * 1024 * 1024];
  auto t = mm.sample();
  auto r = mm.sample();
  auto heap2 = mm.compute_heap2();
  delete[] p;
  auto heap3 = mm.compute_heap3();

  fmt::print("{}:\t{}\n\t\t{}\n\t\t{}\n\theap: {} {} {}\n", __func__, *s, *t, *r, heap1,
             heap2, heap3);
}


#ifndef RUN_THE_BM
int main()
{
  exp_orig();
  exp_new();
}
#endif


// when the creation of the object is part of the benchmark
void BM_ORIG(benchmark::State& state)
{
  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    MemoryModel mm{&stam};
    MemoryModel::snap s;
    mm.sample(&s);
    rss = s.rss;
  }
}
BENCHMARK(BM_ORIG);

void BM_NEW(benchmark::State& state)
{
  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    MM2 mm;
    auto s = mm.sample();
    rss = s->rss;
  }
}
BENCHMARK(BM_NEW);

void BM_NEW_samp2(benchmark::State& state)
{
  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    MM2 mm;
    auto s = mm.sample();
    rss = s->rss;
  }
}
BENCHMARK(BM_NEW_samp2);


// with object creation outside of the loop (and - for mem2, it means the file is opened
// only once)

void BM_ORIG2(benchmark::State& state)
{
  MemoryModel mm{&stam};
  MemoryModel::snap s;
  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    mm.sample(&s);
    rss = s.rss;
  }
}
BENCHMARK(BM_ORIG2);

void BM_NEW2(benchmark::State& state)
{
  MM2 mm;
  [[maybe_unused]] volatile long rss;
  for (auto _ : state) {
    auto s = mm.sample();
    assert(s->rss > 0);
    rss = s->rss;
  }
}
BENCHMARK(BM_NEW2);

// just "compute-heap" for both

void BM_HEAP_ORIG(benchmark::State& state)
{
  MemoryModel mm{&stam};
  MemoryModel::snap s;
  [[maybe_unused]] volatile long hp;
  for (auto _ : state) {
    hp = mm.compute_heap();
  }
}
BENCHMARK(BM_HEAP_ORIG);

void BM_HEAP_NEW(benchmark::State& state)
{
  MM2 mm;
  [[maybe_unused]] volatile long hp;
  for (auto _ : state) {
    hp = mm.compute_heap();
  }
}
BENCHMARK(BM_HEAP_NEW);

void BM_HEAP_NEW2(benchmark::State& state)
{
  MM2 mm;
  [[maybe_unused]] volatile long hp;
  for (auto _ : state) {
    hp = mm.compute_heap2();
  }
}
BENCHMARK(BM_HEAP_NEW2);

void BM_HEAP_NEW3(benchmark::State& state)
{
  MM2 mm;
  [[maybe_unused]] volatile long hp;
  for (auto _ : state) {
    hp = mm.compute_heap3();
  }
}
BENCHMARK(BM_HEAP_NEW3);

#endif

#ifdef RUN_THE_BM

BENCHMARK_MAIN();

#endif
