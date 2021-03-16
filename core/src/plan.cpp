#include <algorithm>
#include <cassert>
#include <iterator>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <oneseismic/geometry.hpp>
#include <oneseismic/messages.hpp>

namespace {

one::gvt< 3 > geometry(
        const nlohmann::json& dimensions,
        const nlohmann::json& shape) noexcept (false) {
    return one::gvt< 3 > {
        { dimensions[0].size(),
          dimensions[1].size(),
          dimensions[2].size(), },
        { shape[0].get< std::size_t >(),
          shape[1].get< std::size_t >(),
          shape[2].get< std::size_t >(), }
    };
}

int task_count(int jobs, int task_size) {
    /*
     * Return the number of task-size'd tasks needed to process all jobs
     */
    const auto x = (jobs + (task_size - 1)) / task_size;
    assert(x != 0);
    if (x <= 0) {
        const auto msg = "task-count < 0; probably integer overflow";
        throw std::runtime_error(msg);
    }
    return x;
}

/*
 * Scheduling
 * ----------
 * Scheduling in this context means the process of:
 *   1. parse an incoming request, e.g. /slice/<dim>/<lineno>
 *   2. build all task descriptions (fragment id + what to extract from
 *      the fragment)
 *   3. split the set of tasks into units of work
 *
 * I/O, the sending of messages to worker nodes is outside this scope.
 *
 * It turns out that the high-level algorithm is largely independent of the
 * task description, so if the "task constructor" is dependency injected then
 * the overall algorithm can be shared between all endpoints.
 *
 * To make matters slightly more complicated, a lot of constraints and
 * functionality is encoded in the types used for messages. It *could*, and
 * still may in the future, be implemented with inheritance, but that approach
 * too comes with its own set of drawbacks.
 *
 * While the types are different, the algorithm *structure* is identical. This
 * makes it a good fit for templates. It comes with some complexity of
 * understanding, but makes adding new endpoints a lot easier, and the reuse of
 * implementation means shared improvements and faster correctness guarantees.
 *
 * This comes with a very real tax for comprehensibility. Templates do add some
 * noise, and the algorithm is split across multiple functions that can all be
 * customised. I anticipate little need for many customisations, but
 * supporting extra customisation points adds very little extra since it just
 * hooks into the machinery required by a single customisation point.
 *
 * The benefit is that adding new endpoints now is a *lot* easier and less
 * error prone.
 */

/*
 * Default implementations and customization points for the scheduling steps.
 * In general, you should only need to implement build() for new endpoints, but
 * partition() and make() are made availble should there be a need to customize
 * them too.
 */
template < typename Input, typename Output >
struct schedule_maker {
    /*
     * Build the schedule - parse the incoming request and build the set of
     * fragment IDs and extraction descriptions. This function is specific to
     * the shape (slice, curtain, horizon etc) and comes with no default
     * implementation.
     *
     * The Output type should have a pack() method that returns a std::string
     */
    Output build(const Input&, const nlohmann::json&) noexcept (false);

    /*
     * Partition partitions an Output in-place and pack()s it into blobs of
     * task_size jobs. It assumes the Output type has a vector-like member
     * called 'ids'. This is a name lookup - should the member be named
     * something else or accessed in a different way then you must implement a
     * custom partition().
     */
    std::vector< std::string >
    partition(Output&, int task_size) noexcept (false);

    /*
     * Make a schedule() - calls build() and partition() in sequence.
     */
    std::vector< std::string >
    schedule(const char* doc, int len, int task_size) noexcept (false);
};

template < typename Input, typename Output >
std::vector< std::string >
schedule_maker< Input, Output >::partition(
        Output& output,
        int task_size
) noexcept (false) {
    if (task_size < 1) {
        const auto msg = fmt::format("task_size (= {}) < 1", task_size);
        throw std::invalid_argument(msg);
    }

    const auto ids = output.ids;
    const auto ntasks = task_count(ids.size(), task_size);

    using std::begin;
    using std::end;
    auto fst = begin(ids);
    auto lst = end(ids);

    std::vector< std::string > xs;
    for (int i = 0; i < ntasks; ++i) {
        const auto last = std::min(fst + task_size, lst);
        output.ids.assign(fst, last);
        std::advance(fst, last - fst);
        xs.push_back(output.pack());
    }

    return xs;
}

template < typename Input, typename Output >
std::vector< std::string >
schedule_maker< Input, Output >::schedule(
        const char* doc,
        int len,
        int task_size)
noexcept (false) {
    Input in;
    in.unpack(doc, doc + len);
    const auto manifest = nlohmann::json::parse(in.manifest);
    auto fetch = this->build(in, manifest);
    return this->partition(fetch, task_size);
}

template <>
one::slice_fetch
schedule_maker< one::slice_task, one::slice_fetch >::build(
    const one::slice_task& task,
    const nlohmann::json& manifest)
{
    auto out = one::slice_fetch(task);

    /*
     * TODO:
     * faster to not make vector, but rather parse-and-compare individual
     * integers?
     */
    const auto& manifest_dimensions = manifest["dimensions"];
    const auto index = manifest_dimensions[task.dim].get< std::vector< int > >();
    const auto itr = std::find(index.begin(), index.end(), task.lineno);
    if (itr == index.end()) {
        const auto msg = "line (= {}) not found in index";
        throw std::invalid_argument(fmt::format(msg, task.lineno));
    }

    const auto pin = std::distance(index.begin(), itr);
    auto gvt = geometry(manifest_dimensions, task.shape);

    // TODO: name loop
    for (const auto& dimension : manifest_dimensions)
        out.shape_cube.push_back(dimension.size());

    const auto to_vec = [](const auto& x) {
        return std::vector< int > { int(x[0]), int(x[1]), int(x[2]) };
    };

    out.lineno = pin % gvt.fragment_shape()[task.dim];
    const auto ids = gvt.slice(gvt.mkdim(task.dim), pin);
    // TODO: name loop
    for (const auto& id : ids)
        out.ids.push_back(to_vec(id));

    return out;
}

template <>
one::curtain_fetch
schedule_maker< one::curtain_task, one::curtain_fetch >::build(
    const one::curtain_task& task,
    const nlohmann::json& manifest)
{
    const auto less = [](const auto& lhs, const auto& rhs) noexcept (true) {
        return std::lexicographical_compare(
            lhs.id.begin(),
            lhs.id.end(),
            rhs.begin(),
            rhs.end()
        );
    };
    const auto equal = [](const auto& lhs, const auto& rhs) noexcept (true) {
        return std::equal(lhs.begin(), lhs.end(), rhs.begin());
    };

    auto out  = one::curtain_fetch(task);
    auto& ids = out.ids;

    const auto& dim0s = task.dim0s;
    const auto& dim1s = task.dim1s;
    auto gvt = geometry(manifest["dimensions"], task.shape);

    const auto zfrags  = gvt.fragment_count(gvt.mkdim(2));
    const auto zheight = gvt.fragment_shape()[2];

    /*
     * Guess the number of coordinates per fragment. A reasonable assumption is
     * a plane going through a fragment, with a little bit of margin. Not
     * pre-reserving is perfectly fine, but we can save a bunch of allocations
     * in the average case by guessing well. It is reasonably short-lived, so
     * overestimating slightly should not be a problem.
     */
    const auto approx_coordinates_per_fragment =
        int(std::max(gvt.fragment_shape()[0], gvt.fragment_shape()[1]) * 1.2);

    /*
     * Pre-allocate the id objects by scanning the input and build the
     * one::single objects, sorted by id lexicographically. All fragments in
     * the column (z-axis) are generated from the x-y pair. This is essentially
     * constructing the "buckets" in advance, as many x/y pairs will end up in
     * the same "bin"/fragment.
     *
     * This is effectively
     *  ids = set([fragmentid(x, y, z) for z in zheight for (x, y) in input])
     *
     * but without any intermediary structures.
     *
     * The bins are lexicographically sorted.
     */
    for (int i = 0; i < int(dim0s.size()); ++i) {
        auto top_point = one::CP< 3 > {
            std::size_t(dim0s[i]),
            std::size_t(dim1s[i]),
            std::size_t(0),
        };
        const auto fid = gvt.frag_id(top_point);

        auto itr = std::lower_bound(ids.begin(), ids.end(), fid, less);
        if (itr == ids.end() or (not equal(itr->id, fid))) {
            one::single top;
            top.id.assign(fid.begin(), fid.end());
            top.coordinates.reserve(approx_coordinates_per_fragment);
            itr = ids.insert(itr, zfrags, top);
            for (int z = 0; z < zfrags; ++z, ++itr)
                itr->id[2] = z;
        }
    }

    /*
     * Traverse the x/y coordinates and put them in the correct bins/fragment
     * ids.
     */
    for (int i = 0; i < int(dim0s.size()); ++i) {
        const auto cp = one::CP< 3 > {
            std::size_t(dim0s[i]),
            std::size_t(dim1s[i]),
            std::size_t(0),
        };
        const auto fid = gvt.frag_id(cp);
        const auto lid = gvt.to_local(cp);
        auto itr = std::lower_bound(ids.begin(), ids.end(), fid, less);
        const auto end = itr + zfrags;
        for (auto task = itr; task != end; ++task) {
            task->coordinates.push_back({ int(lid[0]), int(lid[1]) });
        }
    }

    return out;
}

}

namespace one {

std::vector< std::string >
mkschedule(const char* doc, int len, int task_size) noexcept (false) {
    const auto document = nlohmann::json::parse(doc, doc + len);

    auto slice   = schedule_maker< one::slice_task,   one::slice_fetch >{};
    auto curtain = schedule_maker< one::curtain_task, one::curtain_fetch >{};

    const std::string function = document["function"];
    if (function == "slice") {
        return slice.schedule(doc, len, task_size);
    }
    if (function == "curtain") {
        return curtain.schedule(doc, len, task_size);
    }

    throw std::runtime_error("No handler for function " + function);
}

}
