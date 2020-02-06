#include <catch/catch.hpp>

#include <seismic-cloud/geometry.hpp>
#include "generators.hpp"

using namespace Catch::Matchers;

SCENARIO( "Converting between global and local coordinates" ) {

    GIVEN("A point in global grid is divisible "
          "by the subcube dimensions") {
        sc::CP< 3 > p {100, 200, 110};
        sc::CS< 3 > cube_size {2000, 2000, 1000};
        sc::FS< 3 > frag_size {20, 20, 10};

        const auto co = sc::gvt< 3 >(cube_size, frag_size);

        WHEN("Converting to local coordinates") {
            const auto local = co.to_local(p);

            THEN("The point should be in origo in "
                 "the local coordinate system") {
                CHECK(local == sc::FP< 3 > {0, 0, 0});
            }

            THEN("The point can be converted back to global coordinates") {
                auto root = co.frag_id(p);
                auto result = co.to_global(root, local);
                CHECK(result == p);
            }
        }
    }

    GIVEN( "A point in global grid not divisible "
           "by the fragment dimension< 3 >s" ) {
        sc::CP< 3 > p {55, 67, 88};
        sc::CS< 3 > cube {220, 200, 100};
        sc::FS< 3 > frag {22, 20, 10};

        const auto co = sc::gvt< 3 >(cube, frag);

        WHEN("Converting to local coordinates") {
            const auto local = co.to_local(p);

            THEN("The point is correctly converted to local coordiantes") {
                CHECK(local == sc::FP< 3 > {11, 7, 8});
            }

            THEN("The point can be converted back to global coordiantes") {
                const auto root = co.frag_id(p);
                const auto result = co.to_global(root, local);
                CHECK(result == p);
            }
        }
    }

    GIVEN("Points that should be mapped to the fragment (upper) corners") {
        const sc::CP< 3 > p1 {98, 59, 54};
        const sc::CP< 3 > p2 {65, 79, 109};

        const sc::FS< 3 > frag1 {33, 20, 11};
        const sc::FS< 3 > frag2 {22, 20, 10};

        const sc::CS< 3 > cube {220, 200, 1000};

        const auto co1 = sc::gvt< 3 >(cube, frag1);
        const auto co2 = sc::gvt< 3 >(cube, frag2);

        WHEN("Converting to local coordinates") {
            const auto local1 = co1.to_local(p1);
            const auto local2 = co2.to_local(p2);

            THEN("The point is mapped to the subcubes (upper) corner") {
                CHECK(local1 == sc::FP< 3 > {32, 19, 10});
                CHECK(local2 == sc::FP< 3 > {21, 19, 9});
            }

            THEN("The point can be converted back to global coordinates") {
                const auto root1 = co1.frag_id(p1);
                const auto root2 = co2.frag_id(p2);

                const auto result1 = co1.to_global(root1, local1);
                const auto result2 = co2.to_global(root2, local2);

                CHECK(result1 == p1);
                CHECK(result2 == p2);
            }
        }
    }
}

TEST_CASE("Generate the fragments capturing an inline") {
    auto cube = sc::gvt< 3  >(
        { 9, 15, 23 },
        { 3,  9,  5 }
    );

    CHECK(cube.fragment_count(sc::dimension< 3 >{0}) == 3);
    CHECK(cube.fragment_count(sc::dimension< 3 >{1}) == 2);
    CHECK(cube.fragment_count(sc::dimension< 3 >{2}) == 5);

    const auto result = cube.slice(sc::dimension< 3 >{0}, 0);
    const auto expected = decltype(result) {
        { 0, 0, 0 },
        { 0, 0, 1 },
        { 0, 0, 2 },
        { 0, 0, 3 },
        { 0, 0, 4 },
        { 0, 1, 0 },
        { 0, 1, 1 },
        { 0, 1, 2 },
        { 0, 1, 3 },
        { 0, 1, 4 },
    };

    CHECK_THAT(result, Equals(expected));
}

TEST_CASE("Generate the fragments capturing a crossline") {
    auto cube = sc::gvt< 3 > {
        { 9, 15, 23 },
        { 3,  9,  5 },
    };

    CHECK(cube.fragment_count(sc::dimension< 3 >{0}) == 3);
    CHECK(cube.fragment_count(sc::dimension< 3 >{1}) == 2);
    CHECK(cube.fragment_count(sc::dimension< 3 >{2}) == 5);

    const auto result = cube.slice(sc::dimension< 3 >{1}, 11);
    const auto expected = decltype(result) {
        { 0, 1, 0 },
        { 0, 1, 1 },
        { 0, 1, 2 },
        { 0, 1, 3 },
        { 0, 1, 4 },

        { 1, 1, 0 },
        { 1, 1, 1 },
        { 1, 1, 2 },
        { 1, 1, 3 },
        { 1, 1, 4 },

        { 2, 1, 0 },
        { 2, 1, 1 },
        { 2, 1, 2 },
        { 2, 1, 3 },
        { 2, 1, 4 },
    };

    CHECK_THAT(result, Equals(expected));
}

TEST_CASE("Generate the fragments capturing a time slice") {
    auto cube = sc::gvt< 3 > {
        { 9, 15, 23 },
        { 3,  9,  5 },
    };

    CHECK(cube.fragment_count(sc::dimension< 3 >{0}) == 3);
    CHECK(cube.fragment_count(sc::dimension< 3 >{1}) == 2);
    CHECK(cube.fragment_count(sc::dimension< 3 >{2}) == 5);

    const auto result = cube.slice(sc::dimension< 3 >{2}, 17);
    const auto expected = decltype(result) {
        { 0, 0, 3 },
        { 0, 1, 3 },

        { 1, 0, 3 },
        { 1, 1, 3 },

        { 2, 0, 3 },
        { 2, 1, 3 },
    };
    CHECK_THAT(result, Equals(expected));
}

TEST_CASE("Figure out an global offset [0, len(survey)) from a point") {
    const auto cube = sc::CS< 3 >(9, 15, 23);
    const auto expected = 2495;
    const auto p = sc::CP< 3 >(7, 3, 11);
    auto result = cube.to_offset(p);
    CHECK(result == expected);
}

TEST_CASE("fragment-id string generation") {
    const auto id = sc::FID< 3 >(3, 5, 7);
    CHECK("3-5-7" == id.string());
}

namespace {

const auto exdims = sc::FS< 3 >(3, 5, 7);
const auto exfragment = std::vector< unsigned char > {
    0x0, 0x0, 0x0, 0x0,
    0x0, 0x0, 0x1, 0x0,
    0x0, 0x0, 0x2, 0x0,
    0x0, 0x0, 0x3, 0x0,
    0x0, 0x0, 0x4, 0x0,
    0x0, 0x0, 0x5, 0x0,
    0x0, 0x0, 0x6, 0x0,

    0x0, 0x1, 0x0, 0x0,
    0x0, 0x1, 0x1, 0x0,
    0x0, 0x1, 0x2, 0x0,
    0x0, 0x1, 0x3, 0x0,
    0x0, 0x1, 0x4, 0x0,
    0x0, 0x1, 0x5, 0x0,
    0x0, 0x1, 0x6, 0x0,

    0x0, 0x2, 0x0, 0x0,
    0x0, 0x2, 0x1, 0x0,
    0x0, 0x2, 0x2, 0x0,
    0x0, 0x2, 0x3, 0x0,
    0x0, 0x2, 0x4, 0x0,
    0x0, 0x2, 0x5, 0x0,
    0x0, 0x2, 0x6, 0x0,

    0x0, 0x3, 0x0, 0x0,
    0x0, 0x3, 0x1, 0x0,
    0x0, 0x3, 0x2, 0x0,
    0x0, 0x3, 0x3, 0x0,
    0x0, 0x3, 0x4, 0x0,
    0x0, 0x3, 0x5, 0x0,
    0x0, 0x3, 0x6, 0x0,

    0x0, 0x4, 0x0, 0x0,
    0x0, 0x4, 0x1, 0x0,
    0x0, 0x4, 0x2, 0x0,
    0x0, 0x4, 0x3, 0x0,
    0x0, 0x4, 0x4, 0x0,
    0x0, 0x4, 0x5, 0x0,
    0x0, 0x4, 0x6, 0x0,

    0x1, 0x0, 0x0, 0x0,
    0x1, 0x0, 0x1, 0x0,
    0x1, 0x0, 0x2, 0x0,
    0x1, 0x0, 0x3, 0x0,
    0x1, 0x0, 0x4, 0x0,
    0x1, 0x0, 0x5, 0x0,
    0x1, 0x0, 0x6, 0x0,

    0x1, 0x1, 0x0, 0x0,
    0x1, 0x1, 0x1, 0x0,
    0x1, 0x1, 0x2, 0x0,
    0x1, 0x1, 0x3, 0x0,
    0x1, 0x1, 0x4, 0x0,
    0x1, 0x1, 0x5, 0x0,
    0x1, 0x1, 0x6, 0x0,

    0x1, 0x2, 0x0, 0x0,
    0x1, 0x2, 0x1, 0x0,
    0x1, 0x2, 0x2, 0x0,
    0x1, 0x2, 0x3, 0x0,
    0x1, 0x2, 0x4, 0x0,
    0x1, 0x2, 0x5, 0x0,
    0x1, 0x2, 0x6, 0x0,

    0x1, 0x3, 0x0, 0x0,
    0x1, 0x3, 0x1, 0x0,
    0x1, 0x3, 0x2, 0x0,
    0x1, 0x3, 0x3, 0x0,
    0x1, 0x3, 0x4, 0x0,
    0x1, 0x3, 0x5, 0x0,
    0x1, 0x3, 0x6, 0x0,

    0x1, 0x4, 0x0, 0x0,
    0x1, 0x4, 0x1, 0x0,
    0x1, 0x4, 0x2, 0x0,
    0x1, 0x4, 0x3, 0x0,
    0x1, 0x4, 0x4, 0x0,
    0x1, 0x4, 0x5, 0x0,
    0x1, 0x4, 0x6, 0x0,

    0x2, 0x0, 0x0, 0x0,
    0x2, 0x0, 0x1, 0x0,
    0x2, 0x0, 0x2, 0x0,
    0x2, 0x0, 0x3, 0x0,
    0x2, 0x0, 0x4, 0x0,
    0x2, 0x0, 0x5, 0x0,
    0x2, 0x0, 0x6, 0x0,

    0x2, 0x1, 0x0, 0x0,
    0x2, 0x1, 0x1, 0x0,
    0x2, 0x1, 0x2, 0x0,
    0x2, 0x1, 0x3, 0x0,
    0x2, 0x1, 0x4, 0x0,
    0x2, 0x1, 0x5, 0x0,
    0x2, 0x1, 0x6, 0x0,

    0x2, 0x2, 0x0, 0x0,
    0x2, 0x2, 0x1, 0x0,
    0x2, 0x2, 0x2, 0x0,
    0x2, 0x2, 0x3, 0x0,
    0x2, 0x2, 0x4, 0x0,
    0x2, 0x2, 0x5, 0x0,
    0x2, 0x2, 0x6, 0x0,

    0x2, 0x3, 0x0, 0x0,
    0x2, 0x3, 0x1, 0x0,
    0x2, 0x3, 0x2, 0x0,
    0x2, 0x3, 0x3, 0x0,
    0x2, 0x3, 0x4, 0x0,
    0x2, 0x3, 0x5, 0x0,
    0x2, 0x3, 0x6, 0x0,

    0x2, 0x4, 0x0, 0x0,
    0x2, 0x4, 0x1, 0x0,
    0x2, 0x4, 0x2, 0x0,
    0x2, 0x4, 0x3, 0x0,
    0x2, 0x4, 0x4, 0x0,
    0x2, 0x4, 0x5, 0x0,
    0x2, 0x4, 0x6, 0x0,
};

}

std::vector< unsigned char > slice(sc::slice_layout stride, std::size_t pin) {
    auto outcome = std::vector< unsigned char >();
    const auto start = pin * stride.initial_skip;
    const auto superstride = stride.superstride * sizeof(float);
    const auto chunk_size  = stride.chunk_size  * sizeof(float);
    auto pos = start * sizeof(float);
    for (auto i = 0; i < stride.iterations; ++i) {
        outcome.insert(
            outcome.end(),
            exfragment.begin() + pos,
            exfragment.begin() + pos + chunk_size
        );
        pos += superstride;
    }

    return outcome;
}

TEST_CASE("Extracting a dimension-0 slice from a fragment") {
    const auto expected = [=] {
        auto tmp = std::vector< unsigned char >();
        for (unsigned char i = 0; i < exdims[1]; ++i) {
            for (unsigned char k = 0; k < exdims[2]; ++k) {
                unsigned char t[] = { 0x1, 0x0, 0x0, 0x0, };
                t[1] = i;
                t[2] = k;
                tmp.insert(tmp.end(), std::begin(t), std::end(t));
            }
        }
        return tmp;
    }();

    const auto pin = 1;
    const auto stride = exdims.slice_stride(sc::dimension< 3 >(0));
    const auto outcome = slice(stride, pin);
    CHECK_THAT(outcome, Equals(expected));
}

TEST_CASE("Extracting a dimension-1 slice from a fragment") {
    const auto expected = [=] {
        auto tmp = std::vector< unsigned char >();
        for (unsigned char i = 0; i < exdims[0]; ++i) {
            for (unsigned char k = 0; k < exdims[2]; ++k) {
                unsigned char t[] = { 0x0, 0x1, 0x0, 0x0, };
                t[0] = i;
                t[2] = k;
                tmp.insert(tmp.end(), std::begin(t), std::end(t));
            }
        }
        return tmp;
    }();

    const auto pin = 1;
    const auto stride = exdims.slice_stride(sc::dimension< 3 >(1));
    const auto outcome = slice(stride, pin);
    CHECK_THAT(outcome, Equals(expected));
}

TEST_CASE("Extracting a dimension-2 slice from a fragment") {
    const auto expected = [=] {
        auto tmp = std::vector< unsigned char >();
        for (unsigned char i = 0; i < exdims[0]; ++i) {
            for (unsigned char k = 0; k < exdims[1]; ++k) {
                unsigned char t[] = { 0x0, 0x0, 0x1, 0x0, };
                t[0] = i;
                t[1] = k;
                tmp.insert(tmp.end(), std::begin(t), std::end(t));
            }
        }
        return tmp;
    }();

    const auto pin = 1;
    const auto stride = exdims.slice_stride(sc::dimension< 3 >(2));
    const auto outcome = slice(stride, pin);
    CHECK_THAT(outcome, Equals(expected));
}

TEST_CASE("Put a fragment slice into a cube slice (dimension 0)") {
    const auto expected = std::vector< unsigned char > {
        0x1, 0x0, 0x0, 0x0,
        0x1, 0x0, 0x1, 0x0,
        0x1, 0x0, 0x2, 0x0,
        0x1, 0x0, 0x3, 0x0,
        0x1, 0x0, 0x4, 0x0,
        0x1, 0x0, 0x5, 0x0,
        0x1, 0x0, 0x6, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,

        0x1, 0x1, 0x0, 0x0,
        0x1, 0x1, 0x1, 0x0,
        0x1, 0x1, 0x2, 0x0,
        0x1, 0x1, 0x3, 0x0,
        0x1, 0x1, 0x4, 0x0,
        0x1, 0x1, 0x5, 0x0,
        0x1, 0x1, 0x6, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,

        0x1, 0x2, 0x0, 0x0,
        0x1, 0x2, 0x1, 0x0,
        0x1, 0x2, 0x2, 0x0,
        0x1, 0x2, 0x3, 0x0,
        0x1, 0x2, 0x4, 0x0,
        0x1, 0x2, 0x5, 0x0,
        0x1, 0x2, 0x6, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,

        0x1, 0x3, 0x0, 0x0,
        0x1, 0x3, 0x1, 0x0,
        0x1, 0x3, 0x2, 0x0,
        0x1, 0x3, 0x3, 0x0,
        0x1, 0x3, 0x4, 0x0,
        0x1, 0x3, 0x5, 0x0,
        0x1, 0x3, 0x6, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,

        0x1, 0x4, 0x0, 0x0,
        0x1, 0x4, 0x1, 0x0,
        0x1, 0x4, 0x2, 0x0,
        0x1, 0x4, 0x3, 0x0,
        0x1, 0x4, 0x4, 0x0,
        0x1, 0x4, 0x5, 0x0,
        0x1, 0x4, 0x6, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
    };

    const auto dim0 = sc::dimension< 3 >(0);
    const auto slice_frag_dim = sc::FS< 3 > { 1, 5,  7 };
    const auto slice_dim      = sc::CS< 3 > { 1, 5, 14 };
    auto gvt = sc::gvt< 3 >(slice_dim, slice_frag_dim);
    REQUIRE(expected.size() == gvt.global_size() * sizeof(float));

    /* extract a slice from a fragment */
    const auto pin = 1;
    const auto source_stride = exdims.slice_stride(dim0);
    const auto source = slice(source_stride, pin);

    auto out = expected;
    out.assign(expected.size(), 0);

    /* Put the slice tile at the right place in the output array */
    const auto id = sc::FID< 3 > { 0, 0, 0 };
    auto layout = gvt.slice_stride(dim0, id);
    auto src = source.begin();
    auto dst = out.begin() + layout.initial_skip * sizeof(float);
    for (auto i = 0; i < layout.iterations; ++i) {
        std::copy_n(src, layout.chunk_size * sizeof(float), dst);
        src += layout.substride * sizeof(float);
        dst += layout.superstride * sizeof(float);
    }

    CHECK_THAT(out, Equals(expected));
}

TEST_CASE("Put a fragment slice into a cube slice (dimension 1)") {
    const auto expected = std::vector< unsigned char > {
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x1, 0x0, 0x0,
        0x0, 0x1, 0x1, 0x0,
        0x0, 0x1, 0x2, 0x0,
        0x0, 0x1, 0x3, 0x0,
        0x0, 0x1, 0x4, 0x0,
        0x0, 0x1, 0x5, 0x0,
        0x0, 0x1, 0x6, 0x0,

        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x1, 0x1, 0x0, 0x0,
        0x1, 0x1, 0x1, 0x0,
        0x1, 0x1, 0x2, 0x0,
        0x1, 0x1, 0x3, 0x0,
        0x1, 0x1, 0x4, 0x0,
        0x1, 0x1, 0x5, 0x0,
        0x1, 0x1, 0x6, 0x0,

        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x2, 0x1, 0x0, 0x0,
        0x2, 0x1, 0x1, 0x0,
        0x2, 0x1, 0x2, 0x0,
        0x2, 0x1, 0x3, 0x0,
        0x2, 0x1, 0x4, 0x0,
        0x2, 0x1, 0x5, 0x0,
        0x2, 0x1, 0x6, 0x0,
    };

    const auto dim1 = sc::dimension< 3 >(1);
    const auto slice_frag_dim = sc::FS< 3 > { 3, 1,  7 };
    const auto slice_dim      = sc::CS< 3 > { 3, 1, 14 };
    auto gvt = sc::gvt< 3 >(slice_dim, slice_frag_dim);
    REQUIRE(expected.size() == gvt.global_size() * sizeof(float));

    /* extract a slice from a fragment */
    const auto pin = 1;
    const auto source_stride = exdims.slice_stride(dim1);
    const auto source = slice(source_stride, pin);

    auto out = expected;
    out.assign(expected.size(), 0);

    /* Put the slice tile at the right place in the output array */
    const auto id = sc::FID< 3 > { 0, 0, 1 };
    auto layout = gvt.slice_stride(dim1, id);
    auto src = source.begin();
    auto dst = out.begin() + layout.initial_skip * sizeof(float);
    for (auto i = 0; i < layout.iterations; ++i) {
        std::copy_n(src, layout.chunk_size * sizeof(float), dst);
        src += layout.substride * sizeof(float);
        dst += layout.superstride * sizeof(float);
    }

    CHECK_THAT(out, Equals(expected));
}

TEST_CASE("Put a fragment slice into a cube slice (dimension 1, lateral)") {
    const auto expected = std::vector< unsigned char > {
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,

        0x0, 0x1, 0x0, 0x0,
        0x0, 0x1, 0x1, 0x0,
        0x0, 0x1, 0x2, 0x0,
        0x0, 0x1, 0x3, 0x0,
        0x0, 0x1, 0x4, 0x0,
        0x0, 0x1, 0x5, 0x0,
        0x0, 0x1, 0x6, 0x0,
        0x1, 0x1, 0x0, 0x0,
        0x1, 0x1, 0x1, 0x0,
        0x1, 0x1, 0x2, 0x0,
        0x1, 0x1, 0x3, 0x0,
        0x1, 0x1, 0x4, 0x0,
        0x1, 0x1, 0x5, 0x0,
        0x1, 0x1, 0x6, 0x0,
        0x2, 0x1, 0x0, 0x0,
        0x2, 0x1, 0x1, 0x0,
        0x2, 0x1, 0x2, 0x0,
        0x2, 0x1, 0x3, 0x0,
        0x2, 0x1, 0x4, 0x0,
        0x2, 0x1, 0x5, 0x0,
        0x2, 0x1, 0x6, 0x0,
    };

    const auto dim1 = sc::dimension< 3 >(1);
    const auto slice_frag_dim = sc::FS< 3 > { 3, 1, 7 };
    const auto slice_dim      = sc::CS< 3 > { 6, 1, 7 };
    auto gvt = sc::gvt< 3 >(slice_dim, slice_frag_dim);
    REQUIRE(expected.size() == gvt.global_size() * sizeof(float));

    /* extract a slice from a fragment */
    const auto pin = 1;
    const auto source_stride = exdims.slice_stride(dim1);
    const auto source = slice(source_stride, pin);

    auto out = expected;
    out.assign(expected.size(), 0);

    /* Put the slice tile at the right place in the output array */
    const auto id = sc::FID< 3 > { 1, 0, 0 };
    auto layout = gvt.slice_stride(dim1, id);
    auto src = source.begin();
    auto dst = out.begin() + layout.initial_skip * sizeof(float);
    for (auto i = 0; i < layout.iterations; ++i) {
        std::copy_n(src, layout.chunk_size * sizeof(float), dst);
        src += layout.substride * sizeof(float);
        dst += layout.superstride * sizeof(float);
    }

    CHECK_THAT(out, Equals(expected));
}

TEST_CASE("Put a fragment slice into a cube slice (dimension 2)") {
    const auto expected = std::vector< unsigned char > {
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x0,

        0x0, 0x0, 0x1, 0x0,
        0x0, 0x1, 0x1, 0x0,
        0x0, 0x2, 0x1, 0x0,
        0x0, 0x3, 0x1, 0x0,
        0x0, 0x4, 0x1, 0x0,
        0x1, 0x0, 0x1, 0x0,
        0x1, 0x1, 0x1, 0x0,
        0x1, 0x2, 0x1, 0x0,
        0x1, 0x3, 0x1, 0x0,
        0x1, 0x4, 0x1, 0x0,
        0x2, 0x0, 0x1, 0x0,
        0x2, 0x1, 0x1, 0x0,
        0x2, 0x2, 0x1, 0x0,
        0x2, 0x3, 0x1, 0x0,
        0x2, 0x4, 0x1, 0x0,
    };

    const auto dim2 = sc::dimension< 3 >(2);
    const auto slice_frag_dim = sc::FS< 3 > { 3, 5, 1 };
    const auto slice_dim      = sc::CS< 3 > { 6, 5, 1 };
    auto gvt = sc::gvt< 3 >(slice_dim, slice_frag_dim);
    REQUIRE(expected.size() == gvt.global_size() * sizeof(float));

    /* extract a slice from a fragment */
    const auto pin = 1;
    const auto source_stride = exdims.slice_stride(dim2);
    const auto source = slice(source_stride, pin);

    auto out = expected;
    out.assign(expected.size(), 0);

    /* Put the slice tile at the right place in the output array */
    const auto id = sc::FID< 3 > { 1, 0, 0 };
    auto layout = gvt.slice_stride(dim2, id);
    auto src = source.begin();
    auto dst = out.begin() + layout.initial_skip * sizeof(float);
    for (auto i = 0; i < layout.iterations; ++i) {
        std::copy_n(src, layout.chunk_size * sizeof(float), dst);
        src += layout.substride * sizeof(float);
        dst += layout.superstride * sizeof(float);
    }

    CHECK_THAT(out, Equals(expected));
}