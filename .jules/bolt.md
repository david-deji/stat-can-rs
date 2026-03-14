
## 2024-05-24 - Pre-parsing Query Terms
**Learning:** In text processing and search algorithms, allocating strings (`to_string()`) or performing string transformations (`to_lowercase()`, `split_whitespace()`) inside the inner iteration loop is a significant performance bottleneck. In `score_cube_title_match`, allocating a `Vec<String>` on every call inside a `.filter_map` loop over thousands of cubes caused unnecessary overhead.
**Action:** When filtering or scoring collections against a static query, always pre-process the query (e.g., lowercase it, split into terms, collect into slices of references `&[&str]`) outside the loop, and pass the pre-computed references to the inner matching function.
