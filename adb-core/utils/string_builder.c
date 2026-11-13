/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
============================================================================
 Author: Danil Lipovoy
 Notice: Certain algorithms, data structures, and methods implemented in this software are disclosed in U.S. Provisional Patent Application No. 64/161,408, filed September 24, 2026.
 This software is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0) and nothing in this notice is intended to modify the rights granted under that license.
============================================================================

 */
#include "string_builder.h"
char *digit_grouping(uint64_t val, char *out, size_t out_size) {
    if (!out || out_size == 0) {
        return NULL;
    }
    char tmp[32];
    int len = snprintf(tmp, sizeof(tmp), "%lu", (unsigned long)val);

    if (len <= 0) {
        if (out_size > 0) {
            out[0] = '\0';
        }
        return out;
    }

    if (len >= (int)sizeof(tmp)) {
        len = (int)sizeof(tmp) - 1;
        tmp[len] = '\0';
    }
    size_t write_pos = out_size - 1;
    out[write_pos] = '\0';
    int digits_written = 0;

    for (int i = len - 1; i >= 0; --i) {
        if (write_pos == 0) {
            break;
        }
        out[--write_pos] = tmp[i];
        digits_written++;

        if (i > 0 && (digits_written % 3) == 0) {
            if (write_pos == 0) {
                break;
            }
            out[--write_pos] = '\'';
        }
    }
    size_t result_len = (out_size - 1) - write_pos;

    if (write_pos > 0 && result_len > 0) {
        memmove(out, out + write_pos, result_len + 1);
    }
    return out;
}


int split(char *str, char delim, char ***array) {
    char *p;
    char **res;
    int count = 0;
    int k = 0;
    p = str;

    while ((p = strchr(p, delim)) != NULL) {
        *p = 0;
        p++;
        count++;
    }
    count++;
    res = calloc(1, count * sizeof(char *));

    if (!res)
        return -1;
    p = str;

    for (k = 0; k < count; k++) {
        if (*p)
            res[k] = p;
        p = strchr(p, 0);
        p++;
    }
    *array = res;
    return count;
}

/*
size_t get_no_tokens(char *str, const char *split) {
    const char *p;
    size_t count = 1;
    size_t len = strlen(split);
    if (len == 0)
        return strlen(str);
    for (p = str; (p = strstr(p, split)) != NULL; p += len)
        count++;
    return count;
}
char **split_string(char *str, const char *split, uint32_t rsize) {
    size_t count = get_no_tokens(str, split);
    char **result = malloc(2 * rsize + sizeof(char)); // TODO not x2
    size_t len = strlen(split);
    size_t idx;
    const char *p = str;
    for (idx = 0; idx < count; idx++) {
        const char *q = strstr(p, split);
        if (q == NULL) {
            q = p + strlen(p);
        } else
        if (q == p && *q != '\0') {
            q++;
        }
        result[idx] = strndup(p, q - p);
        result[idx][q - p] = 0;
        p = q + len;
    }
    result[idx] = NULL;
    return result;
}
void print_split_string(char **split_string) {
    for (size_t i = 0; split_string[i] != NULL; i++) {
        printf("%s\n", split_string[i]);
    }
}
void free_split_string(char **split_string) {
    for (size_t i = 0; split_string[i] != NULL; i++) {
        free(split_string[i]);
    }
    free(split_string);
}
*/
