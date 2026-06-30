package com.covertchannel.framework;

public class CephaConfig {

    // -- Output --
    public static final String ENV_OUTPUT_PATH                = "CEPHA_OUTPUT_PATH";
    public static final String ENV_DLQ_PATH                   = "CEPHA_DLQ_PATH";
    public static final String ENV_OUTPUT_ROLLOVER_INTERVAL   = "CEPHA_OUTPUT_ROLLOVER_SECONDS";
    public static final String ENV_OUTPUT_INACTIVITY_INTERVAL = "CEPHA_OUTPUT_INACTIVITY_SECONDS";
    public static final String ENV_OUTPUT_MAX_SIZE_MB         = "CEPHA_OUTPUT_MAX_SIZE_MB";

    // -- Defaults --
    public static final long DEFAULT_ROLLOVER_SEC   = 60;
    public static final long DEFAULT_INACTIVITY_SEC = 60;
    public static final long DEFAULT_MAX_SIZE_MB    = 100;
}
