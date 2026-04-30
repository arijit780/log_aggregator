#pragma once

/** Umbrella include for applications that want the full public surface. */

// Week 1-2: single-file log with pluggable codec
#include "log_storage/crypto/crc32.hpp"
#include "log_storage/durability/durability_manager.hpp"
#include "log_storage/durability/durability_mode.hpp"
#include "log_storage/format/irecord_codec.hpp"
#include "log_storage/format/record_decode_status.hpp"
#include "log_storage/format/v1_binary_codec.hpp"
#include "log_storage/format/v1_constants.hpp"
#include "log_storage/io/byte_io.hpp"
#include "log_storage/reader/log_reader.hpp"
#include "log_storage/recovery/recovery_manager.hpp"
#include "log_storage/writer/durable_log_writer.hpp"
#include "log_storage/writer/log_writer.hpp"

// Week 3-4: segment-based log with consumer offset tracking
#include "log_storage/consumer/offset_store.hpp"
#include "log_storage/storage/durability.hpp"
#include "log_storage/storage/log.hpp"
#include "log_storage/storage/record.hpp"
#include "log_storage/storage/recovery.hpp"
#include "log_storage/storage/segment.hpp"
