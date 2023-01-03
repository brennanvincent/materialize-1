// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::collapsible_if)]
#![warn(clippy::collapsible_else_if)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

//! Utility crate to get the linker-supplied build ID
//! of all loaded images.
//!
//! Currently only works on Linux

// TODO(btv): Document why the `as` conversions in this function are legit
#![allow(clippy::as_conversions)]

/// Gets the GNU build IDs for all loaded images, including the main
/// program binary as well as all dynamically loaded libraries.
/// Intended to be useful for profilers, who can use the supplied IDs
/// to symbolicate stack traces offline.
///
/// Uses `dl_iterate_phdr` to walk the program headers of all images,
/// and iterates over them looking for note
/// segments. Then searches the discovered note segments for a note of type
/// `NT_GNU_BUILD_ID` (aka "3") and name "GNU\0".
///
/// SAFETY: This function is written in a hilariously unsafe way: it involves
/// following pointers to random parts of memory, and then assuming
/// that particular structures can be found there.
/// However, it was written by carefully reading `man dl_iterate_phdr`
/// and `man elf`, and is thus intended to be relatively safe for callers to use.
/// Assuming I haven't written any bugs (and that the documentation is correct),
/// the only known safety requirements are:
///
/// (1) It must not be called multiple times concurrently, as `dl_iterate_phdr`
/// is not documented as being thread-safe.
/// (2) The running binary must be in ELF format and running on Linux.
#[cfg(target_os = "linux")]
pub unsafe fn all_build_ids(
) -> Result<std::collections::HashMap<std::path::PathBuf, Vec<u8>>, anyhow::Error> {
    // local imports to avoid polluting the namespace for macOS builds
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;
    use std::ffi::{c_int, CStr, OsStr};
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Path, PathBuf};

    use anyhow::Context;
    use libc::{c_void, dl_iterate_phdr, dl_phdr_info, size_t, Elf64_Word, PT_NOTE};

    use mz_ore::bits::align_up;
    use mz_ore::cast::CastFrom;

    struct CallbackState {
        map: HashMap<PathBuf, Vec<u8>>,
        is_first: bool,
        fatal_error: Option<anyhow::Error>,
    }

    // TODO(benesch): rewrite to avoid potentially dangerous usage of `as`.
    #[allow(clippy::as_conversions)]
    extern "C" fn iterate_cb(info: *mut dl_phdr_info, _size: size_t, data: *mut c_void) -> c_int {
        // SAFETY: `data` is a pointer to a `CallbackState`, and no mutable reference
        // aliases with it in Rust. Furthermore, `dl_iterate_phdr` doesn't do anything
        // with `data` other than pass it to this callback, so nothing will be mutating
        // the object it points to while we're inside here.
        let state: &mut CallbackState = unsafe {
            (data as *mut CallbackState)
                .as_mut()
                .expect("`data` cannot be null")
        };
        // SAFETY: similarly, `dl_iterate_phdr` isn't mutating `info`
        // while we're here.
        let info = unsafe { info.as_ref() }.expect("`dl_phdr_info` cannot be null");
        let fname = if state.is_first {
            // From `man dl_iterate_phdr`:
            // "The first object visited by callback is the main program.  For the main
            // program, the dlpi_name field will be an empty string."
            match std::env::current_exe()
                .context("failed to read the name of the current executable")
            {
                Ok(pb) => Some(pb),
                Err(e) => {
                    // Profiles will be of dubious usefulness
                    // if we can't get the build ID for the main executable,
                    // so just bail here.
                    state.fatal_error = Some(e);
                    return -1;
                }
            }
        } else if info.dlpi_name.is_null() {
            None
        } else {
            // SAFETY: `dl_iterate_phdr` documents this as being a null-terminated string.
            let fname = unsafe { CStr::from_ptr(info.dlpi_name) };
            Some(Path::new(OsStr::from_bytes(fname.to_bytes())).to_path_buf())
        };
        state.is_first = false;
        if let Some(fname) = fname {
            if let Entry::Vacant(ve) = state.map.entry(fname) {
                // Walk the headers of this image, looking for a segment containing notes

                // SAFETY: `dl_iterate_phdr` is documented as setting `dlpi_phnum` to the
                // length of the array pointed to by `dlpi_phdr`.
                let program_headers =
                    unsafe { std::slice::from_raw_parts(info.dlpi_phdr, info.dlpi_phnum.into()) };
                let mut found_build_id = None;
                'outer: for ph in program_headers {
                    if ph.p_type == PT_NOTE {
                        // From `man elf`:
                        // typedef struct {
                        //   Elf64_Word n_namesz;
                        //   Elf64_Word n_descsz;
                        //   Elf64_Word n_type;
                        // } Elf64_Nhdr;
                        #[repr(C)]
                        struct NoteHeader {
                            n_namesz: Elf64_Word,
                            n_descsz: Elf64_Word,
                            n_type: Elf64_Word,
                        }
                        // This is how `man dl_iterate_phdr` says to find the segment headers in memory.
                        let mut offset = usize::cast_from(ph.p_vaddr + info.dlpi_addr);
                        let orig_offset = offset;

                        const NT_GNU_BUILD_ID: Elf64_Word = 3;
                        const GNU_NOTE_NAME: &[u8; 4] = b"GNU\0";
                        const ELF_NOTE_STRING_ALIGN: usize = 4;

                        while offset + std::mem::size_of::<NoteHeader>() + GNU_NOTE_NAME.len()
                            <= orig_offset + usize::cast_from(ph.p_memsz)
                        {
                            let nh = unsafe { (offset as *const NoteHeader).as_ref() }
                                .expect("the program headers must be well-formed");
                            // from elf.h
                            if nh.n_type == NT_GNU_BUILD_ID
                                && nh.n_descsz != 0
                                && usize::cast_from(nh.n_namesz) == GNU_NOTE_NAME.len()
                            {
                                let p_name =
                                    (offset + std::mem::size_of::<NoteHeader>()) as *const [u8; 4];
                                // SAFETY: since `n_namesz` is 4, the name is a four-byte value.
                                let name = unsafe { p_name.as_ref() }.expect("this can't be null");
                                if name == GNU_NOTE_NAME {
                                    // We found what we're looking for!
                                    let p_desc = (p_name as usize + 4) as *const u8;
                                    // SAFETY: This is the documented meaning of `n_descsz`.
                                    let desc = unsafe {
                                        std::slice::from_raw_parts(
                                            p_desc,
                                            usize::cast_from(nh.n_descsz),
                                        )
                                    };
                                    found_build_id = Some(desc.to_vec());
                                    break 'outer;
                                }
                            }
                            offset = offset
                                + std::mem::size_of::<NoteHeader>()
                                + align_up::<ELF_NOTE_STRING_ALIGN>(usize::cast_from(nh.n_namesz))
                                + align_up::<ELF_NOTE_STRING_ALIGN>(usize::cast_from(nh.n_descsz));
                        }
                    }
                }
                if let Some(build_id) = found_build_id {
                    ve.insert(build_id);
                }
            }
        }
        0
    }
    let mut state = CallbackState {
        map: HashMap::new(),
        is_first: true,
        fatal_error: None,
    };
    // SAFETY: `dl_iterate_phdr` has no documented restrictions on when
    // it can be called.
    unsafe {
        dl_iterate_phdr(
            Some(iterate_cb),
            (&mut state) as *mut CallbackState as *mut c_void,
        );
    };
    if let Some(err) = state.fatal_error {
        Err(err)
    } else {
        Ok(state.map)
    }
}
