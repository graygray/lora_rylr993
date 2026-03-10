#!/usr/bin/env python3
import argparse
import serial
import time
import sys
from typing import Optional, Tuple

def resolve_port(port_arg: Optional[str], default="/dev/ttyUSB0") -> str:
    if port_arg is None:
        return default
    s = str(port_arg).strip()
    if s.isdigit():
        return f"/dev/ttyUSB{s}"
    return s

def parse_bw_to_code(bw: str) -> int:
    s = str(bw).lower().strip()
    if s in ("7", "8", "9"):
        return int(s)
    s = s.replace("khz", "k").replace(" ", "")
    if s.endswith("k"):
        s = s[:-1]
    if s in ("125", "125000"):
        return 7
    if s in ("250", "250000"):
        return 8
    if s in ("500", "500000"):
        return 9
    raise ValueError(f"Unsupported --bw {bw}. Use 7/8/9 or 125/250/500 (kHz).")

def write_cmd(ser: serial.Serial, cmd: str, verbose: bool):
    if verbose:
        print(f"[TX] {cmd}")
    ser.write((cmd + "\r\n").encode())

def at_collect(ser: serial.Serial, cmd: str, wait_s: float, verbose: bool):
    write_cmd(ser, cmd, verbose)
    end = time.time() + wait_s
    while time.time() < end:
        l = ser.readline().decode(errors="ignore").strip()
        if l and verbose:
            print("[AT]", l)

def wait_ready(ser: serial.Serial, timeout_s: float, verbose: bool) -> bool:
    end = time.time() + timeout_s
    while time.time() < end:
        l = ser.readline().decode(errors="ignore").strip()
        if l:
            if verbose:
                print("[RST]", l)
            if "+READY" in l:
                return True
    return False

def drain_uart(ser: serial.Serial, seconds: float, verbose: bool):
    end = time.time() + seconds
    while time.time() < end:
        l = ser.readline().decode(errors="ignore").strip()
        if l and verbose:
            print("[DRAIN]", l)

def init_radio_robot(
    ser: serial.Serial,
    my_id: int,
    band_hz: int,
    sf: int,
    bw_code: int,
    cr: int,
    preamble: int,
    crfop: int,
    verbose: bool,
):
    at_collect(ser, "AT", 0.3, verbose)

    write_cmd(ser, "AT+OPMODE=1", verbose)
    time.sleep(0.6)

    need_reset = False
    while True:
        l = ser.readline().decode(errors="ignore").strip()
        if not l:
            break
        if verbose:
            print("[AT]", l)
        if "Need RESET" in l:
            need_reset = True

    if need_reset:
        write_cmd(ser, "ATZ", verbose)
        wait_ready(ser, 4.0, verbose)

    at_collect(ser, f"AT+ADDRESS={my_id}", 0.5, verbose)
    at_collect(ser, f"AT+BAND={band_hz}", 0.5, verbose)
    at_collect(ser, f"AT+PARAMETER={sf},{bw_code},{cr},{preamble}", 0.6, verbose)
    at_collect(ser, f"AT+CRFOP={crfop}", 0.5, verbose)

    at_collect(ser, "AT+PARAMETER=?", 0.8, verbose)
    drain_uart(ser, 0.8, verbose)

def parse_rcv(line: str) -> Optional[Tuple[int, int, str, int, int]]:
    if not line.startswith("+RCV="):
        return None
    try:
        p = line[5:].split(",", 4)
        src = int(p[0]); ln = int(p[1]); data = p[2]
        rssi = int(p[3]); snr = int(p[4])
        return src, ln, data, rssi, snr
    except:
        return None

def make_payload_ascii(my_id: int, frame_id: int, payload_bytes: int) -> str:
    """
    ASCII payload to avoid binary truncation issues.
    First 6 bytes: rid(2 hex) + fid(4 hex) => "020001"
    """
    if payload_bytes < 6:
        raise ValueError("--payload-bytes must be >= 6.")
    header = f"{my_id:02x}{frame_id:04x}"
    dummy_len = payload_bytes - 6
    return header + ("a" * dummy_len)

def sleep_until(deadline_mono: float, busy_tail_s: float = 0.002):
    """
    Sleep until a monotonic deadline with low jitter.
    - For most of remaining time, sleep.
    - Last busy_tail_s uses busy-wait (more CPU but much tighter timing).
    """
    while True:
        now = time.monotonic()
        remain = deadline_mono - now
        if remain <= 0:
            return
        if remain > busy_tail_s:
            time.sleep(remain - busy_tail_s)
        else:
            # busy wait
            pass

def parse_args():
    ap = argparse.ArgumentParser("TDMA Robot (calibrated + low-jitter wait)")

    ap.add_argument("--port", default=None, help="Serial port: 0->/dev/ttyUSB0 or /dev/ttyUSBX")
    ap.add_argument("--baud", type=int, default=9600)
    ap.add_argument("--robotid", type=int, required=True, help="Robot ID / LoRa ADDRESS")
    ap.add_argument("--master", type=int, default=1)
    ap.add_argument("--band", type=int, default=915000000)
    ap.add_argument("--crfop", type=int, default=22)

    ap.add_argument("--sf", type=int, default=5)
    ap.add_argument("--bw", default="9")
    ap.add_argument("--cr", type=int, default=1)
    ap.add_argument("--preamble", type=int, default=12)

    ap.add_argument("--slot", type=float, default=0.1)
    ap.add_argument("--base-delay", type=float, default=0.25)
    ap.add_argument("--tx-offset", type=float, default=0.02)

    ap.add_argument("--payload-bytes", type=int, default=32,
                    help="TX payload length in BYTES (ASCII). Must be >=6. "
                         "First 6 bytes are rid+fid hex, rest dummy printable.")

    # ★ key: compensate delay from real RF RX time to host seeing '+RCV=' line
    ap.add_argument("--rx-delay-ms", type=float, default=190.0,
                    help="Compensate latency between actual RF RX time and when '+RCV=' line is read by this program. "
                         "Example: 175.0")

    ap.add_argument("--busy-tail-ms", type=float, default=2.0,
                    help="Busy-wait tail time (ms) for precise slot timing. 1~3ms recommended.")

    ap.add_argument("--quiet", action="store_true")
    return ap.parse_args()

def main():
    args = parse_args()
    if args.robotid <= 0 or args.robotid > 65535:
        print("[ERR] --robotid must be 1..65535")
        sys.exit(2)
    if args.payload_bytes < 6:
        print("[ERR] --payload-bytes must be >= 6")
        sys.exit(2)

    verbose = not args.quiet
    bw_code = parse_bw_to_code(args.bw)
    port = resolve_port(args.port)

    if verbose:
        print(f"[CFG] PORT={port} BAUD={args.baud} MY_ID={args.robotid} MASTER={args.master} "
              f"sf={args.sf} bw_code={bw_code} cr={args.cr} pre={args.preamble} "
              f"slot={args.slot:.3f}s base={args.base_delay:.3f}s off={args.tx_offset:.3f}s "
              f"payload={args.payload_bytes}B rx_delay={args.rx_delay_ms:.1f}ms busy_tail={args.busy_tail_ms:.1f}ms")

    ser = serial.Serial(port, args.baud, timeout=0.1)
    init_radio_robot(
        ser=ser,
        my_id=args.robotid,
        band_hz=args.band,
        sf=args.sf,
        bw_code=bw_code,
        cr=args.cr,
        preamble=args.preamble,
        crfop=args.crfop,
        verbose=verbose,
    )

    if verbose:
        print("TDMA Robot (calibrated) running...")

    last_frame = None
    rx_delay_s = args.rx_delay_ms / 1000.0
    busy_tail_s = max(0.0, args.busy_tail_ms / 1000.0)

    while True:
        line = ser.readline().decode(errors="ignore").strip()
        if not line:
            continue

        r = parse_rcv(line)
        if not r:
            continue

        src, ln, data, rssi, snr = r

        if not (data.startswith("BCN") and len(data) >= 7):
            continue

        try:
            frame = int(data[3:7])
        except:
            continue

        if frame == last_frame:
            continue
        last_frame = frame

        # Timestamp when we *read* the +RCV line
        read_m = time.monotonic()

        # Compensated estimate of actual RF RX time
        beacon_rx_m = read_m - rx_delay_s

        # Schedule TX in our slot based on compensated time
        tx_time = beacon_rx_m + args.base_delay + (args.robotid - 1) * args.slot + args.tx_offset

        # If already too late, skip (use compensated now)
        now_m = time.monotonic()
        if now_m > tx_time + 0.03:
            if verbose:
                late_ms = (now_m - beacon_rx_m) * 1000.0
                print(f"[SKIP] late frame={frame} now-beacon={late_ms:.1f}ms (rx_delay={args.rx_delay_ms:.1f}ms)")
            continue

        sleep_until(tx_time, busy_tail_s=busy_tail_s)

        payload_ascii = make_payload_ascii(args.robotid, frame, args.payload_bytes)
        write_cmd(ser, f"AT+SEND={args.master},{len(payload_ascii)},{payload_ascii}", verbose)

        if verbose:
            after_ms = (time.monotonic() - beacon_rx_m) * 1000.0
            print(f"[SENT] frame={frame} after_beacon={after_ms:.1f}ms RSSI={rssi} SNR={snr} (rx_delay={args.rx_delay_ms:.1f}ms)")

if __name__ == "__main__":
    main()

