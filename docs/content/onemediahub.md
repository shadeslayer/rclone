---
title: "OneMediaHub"
description: "Rclone docs for OneMediaHub and O2 Cloud"
versionIntroduced: "v1.76"
---

# OneMediaHub

This backend connects to Funambol OneMediaHub servers, including O2 Cloud.
Paths use `remote:directory/file`.

## O2 Cloud Spain

Run `rclone config`, create a remote, and select `onemediahub`.
Keep the default server URL and choose `oauth` authentication.
Supply your provider's OAuth client ID and client secret. Proprietary
credentials from the O2 desktop application are not bundled with rclone.
The configured client must permit the selected callback URL and offline access.

Open the authorization URL printed by rclone. After signing in, copy the
complete callback URL from the browser address bar into rclone. The default
callback is O2's desktop callback, so the browser page may remain loading.
Rclone validates the callback state and exchanges the code using PKCE.

Rclone saves the access and refresh tokens in its configuration. It refreshes
expired tokens, recreates expired server sessions, and saves replacement
credentials returned by OneMediaHub. Subsequent commands do not require a
browser login while the provider accepts the saved refresh token. Revoked or
expired refresh tokens require `rclone config reconnect remote:`.
Use a writable configuration file to retain refreshed credentials across runs.
Rclone also saves a stable `device_id` for the server's client identification.
Like the Windows client, it saves the SAPI session and validation key for reuse
across commands. Cached sessions are bound to the server, client and credentials;
expired sessions are renewed automatically.
Requests reuse the session cookie and optional validation key. Like O2's
desktop and Android clients, OAuth requests also send the token envelope
and save refreshed tokens from response headers. Passwords are sent only at login.

## Other OneMediaHub servers

Set `url` to your server, for example `https://cloud.example.com` or
`https://example.com/cloud`. Include any deployment prefix but omit `/sapi`.
Set `api_path` if the server uses a different SAPI path.

For a server supporting password login:

```console
rclone config
```

Select `onemediahub`, enter the server URL, choose `password`, and supply
`user` and `password`. Rclone saves the password obscured and renews sessions
without prompting.

For Zefiro, use `https://zefiro.me` and password authentication. Enter the phone
number with its country code, using digits only. Leave `device_id` empty to
generate a persistent native client ID. No browser cookie or OAuth client ID
is needed.

For OAuth, set the provider's `client_id`, `client_secret` if required,
`auth_url`, `token_url`, `redirect_url`, and `scope` in advanced configuration.
`platform` and `msisdn` are available for provider-specific adapters.
Set `user_agent` if the provider requires a particular HTTP client identity.
O2 Spain and Germany have separate endpoint and scope defaults. For Germany,
use `https://cloud.o2.de` and its registered client credentials. Rclone adds
the Android app's `client_type=omh` authorization parameter. Its APK and public
discovery endpoint have been checked; account login still needs live testing.

The server's public system-information API supplies its upload endpoint.
`upload_url` overrides that endpoint; otherwise uploads use `url` when the
server does not advertise a separate upload host.

O2 also supports a two-stage upload protocol used by its Windows client.
Enable `async_upload` in advanced configuration, or use
`--onemediahub-async-upload`. Rclone registers metadata, sends raw file
content, then polls the server's validation status until processing completes.
Independent files can use this protocol concurrently with `--transfers`.
The server processing wait defaults to five minutes and can be adjusted with
`--onemediahub-upload-timeout`. A failed or interrupted upload can leave a
metadata-only item on the server; rclone reports the media ID in content and
processing errors. Multipart uploads remain the default for compatibility
with other OneMediaHub deployments.

To reduce metadata requests during copy and sync, enable `metadata_cache`, or
use `--onemediahub-metadata-cache`. Rclone uses the same changes API as the O2
Windows client and fetches changed media IDs in batches. It stores the account's
metadata in rclone's cache directory using the shared `lib/kv` framework, so
later runs can fetch changes instead of rebuilding the account listing.
Systems without persistent-store support keep the cache in memory.

The changes API is checked at startup and then at most once per minute when
metadata is needed. Adjust the interval with `--onemediahub-metadata-cache-time`.
Changes made by other clients may remain invisible during that interval.
Successful rclone uploads and deletions update the cache immediately, and
downloads always request fresh metadata for their URLs. Caching is disabled
by default because it requires the profile and changes APIs. It reduces API
traffic but does not establish whether a particular HTTP 403 is throttling.

File deletions are batched by default, grouped by media type, and confirmed
before rclone reports success for each file. Batches contain at most 1,000
entries and are limited by `--checkers`; smaller batches flush after 20 ms
of inactivity. Set `--onemediahub-delete-batch-size` to reduce the maximum,
or set it to `1` for individual requests. If an already-trashed file rejects
a batch, rclone retries its members individually to identify failing files
and delete the others. A file confirmed as already in the trash counts as
deleted. Other failures, including network, quota and permission errors,
are returned without retrying the batch. Folder deletion continues to
require an empty folder.

A configured remote's server can also be selected with
`--onemediahub-url https://cloud.example.com`. Its saved credentials must
belong to that server. Use separate remotes for separate accounts or servers.

## Usage

```console
rclone lsd remote:
rclone ls remote:
rclone copy ./photos remote:photos
rclone copy remote:photos ./download
rclone about remote:
```

By default, the root contains top-level folders and unfiled media. The server's
`parentid` relationships determine the hierarchy; watch folders (`magic`)
are ordinary directories. Set `root_folder_id` to use a specific folder.

O2 may place its folders inside a directory named `/`. Rclone represents a
slash inside a name as `／` (U+FF0F), so list its children with:

```console
rclone lsd 'remote:／'
```

`lsd` lists directories at one level. Use `lsd -R` for all directories or
`ls` for files.

The O2 Windows client selects the `magic` folder returned by
`/media/folder/root?action=get`, an endpoint absent from the 14.5 guide.
Set `root_folder_id` to that folder's ID to match its view. Rclone's default
view preserves other top-level folders and unfiled media.

## Limitations

- Uploads require a known size. Timestamps are preserved to the nearest second
  when uploading; changing timestamps without uploading is unsupported.
- File deletion moves items to the server's trash. Empty folders are deleted.
- Leading and trailing periods are encoded because some upload servers reject
  dotfiles or strip trailing periods.
- The API does not specify a content-checksum contract for ETags, so this
  backend does not report hashes.
- Listing uses the paginated folder and generic-media APIs and filters by
  parent locally. Large accounts may require many requests.
- Server-side copy, move, and sharing are not implemented.

The protocol follows the OneMediaHub 14.5 Server API Developer's Guide.
Tests cover its request formats, mixed numeric/string IDs, folder hierarchy,
top-level pagination flag, optional validation keys, and session authentication.
O2 compatibility tests separately cover nested pagination flags, repeated root
folders, dotfile encoding, and refreshed OAuth response headers observed in its
Windows and Android clients. Zefiro tests cover native device identification
and downloads of empty files. Upload-server discovery is also a deployment
extension.
Other deployments require integration testing against their servers.

<!-- autogenerated options start -->
<!-- autogenerated options stop -->
