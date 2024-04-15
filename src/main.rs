use std::collections::HashMap;
use std::ffi::c_void;
use std::mem::size_of;
use std::time;
use windows::core::PCWSTR;
use windows::Win32::Foundation::{CloseHandle, ERROR_HANDLE_EOF, GENERIC_READ, GetLastError};
use windows::Win32::Storage::FileSystem::{CreateFileW, FILE_FLAGS_AND_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::Ioctl::{FSCTL_ENUM_USN_DATA, FSCTL_QUERY_USN_JOURNAL, MFT_ENUM_DATA_V0, USN_JOURNAL_DATA_V0, USN_RECORD_V2};

fn main() -> windows::core::Result<()> {
    let cost = time::Instant::now();
    unsafe {
        let path = r#"\\.\D:"#.encode_utf16().chain(Some(0)).collect::<Vec<u16>>().as_ptr();
        let dh = CreateFileW(PCWSTR(path),
                             GENERIC_READ.0,
                             FILE_SHARE_READ | FILE_SHARE_WRITE,
                             None,
                             OPEN_EXISTING,
                             FILE_FLAGS_AND_ATTRIBUTES(0),
                             None)?;

        let mut usn_data = USN_JOURNAL_DATA_V0::default();
        DeviceIoControl(dh,
                        FSCTL_QUERY_USN_JOURNAL,
                        None,
                        0,
                        Some(&mut usn_data as *mut _ as *mut c_void),
                        size_of::<USN_JOURNAL_DATA_V0>() as u32,
                        None,
                        None)?;

        let mut mft_data = MFT_ENUM_DATA_V0::default();
        mft_data.StartFileReferenceNumber = 0;
        mft_data.LowUsn = 0;
        mft_data.HighUsn = usn_data.MaxUsn;

        const BUF_SIZE: u32 = 32768u32;
        let mut idx = 0;
        let mut times = 0;
        let mut fs_map: HashMap<u64, Vec<USN_RECORD_V2>> = HashMap::new();

        let buf: Vec<u8> = Vec::with_capacity(BUF_SIZE as usize);
        loop {
            let mut returned = 0u32;
            let mut usn_record = buf.as_ptr() as *mut USN_RECORD_V2;
            if let Err(er) = DeviceIoControl(dh,
                                             FSCTL_ENUM_USN_DATA,
                                             Some(&mut mft_data as *mut _ as *mut c_void),
                                             size_of::<MFT_ENUM_DATA_V0>() as u32,
                                             Some(buf.as_ptr() as *mut c_void),
                                             BUF_SIZE,
                                             Some(&mut returned as *mut u32),
                                             None) {
                if GetLastError() != ERROR_HANDLE_EOF {
                    eprintln!("Device io control: {}", er);
                }
                break;
            }
            times += 1;

            usn_record = usn_record.byte_add(size_of::<i64>());
            let end_ptr = buf.as_ptr().byte_add(returned as usize);

            while (usn_record as u64) < (end_ptr as u64) {
                // let filename = String::from_utf16(slice::from_raw_parts((*usn_record).FileName.as_ptr(), ((*usn_record).FileNameLength / 2) as usize));
                // println!("id: 0x{:08x}, name: {}", (*usn_record).FileReferenceNumber, filename.unwrap());
                fs_map.entry((*usn_record).ParentFileReferenceNumber).or_insert_with(Vec::new).push((*usn_record).clone());
                usn_record = usn_record.byte_add((*usn_record).RecordLength as usize);
                idx += 1;
            };

            mft_data.StartFileReferenceNumber = *(buf.as_ptr() as *const u64);
        }
        println!("total entry: {}, times: {}\n", idx, times);

        CloseHandle(dh)?;
    };
    println!("cost: {:?}", cost.elapsed());
    Ok(())
}
