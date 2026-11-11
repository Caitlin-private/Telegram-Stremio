# Manual Media Addition JSON Format

Use **Media Management → Add Content Manually → Paste JSON batch** to add one or many movies/series without filling the form repeatedly.

An AI can generate this JSON from a list of titles and Telegram links. Give it this document and your source details, and ask it to return valid JSON only.

## Accepted top-level formats

A single title object:

```json
{
  "media_type": "tv",
  "stream": {
    "parts": [
      {"url": "https://t.me/c/4486527497/124-https://t.me/c/4486527497/142"}
    ]
  },
  "manual_metadata": {
    "title": "Prep: ENT RR",
    "year": 2026,
    "genres": ["FMGE"],
    "description": "Prepladder RR XI",
    "poster": "https://drive.fowlers.site/0:/Misc/prep.jpeg"
  },
  "episode_detection": "order",
  "season_number": 11
}
```

An array of independent titles, or an object containing an `items` array, is also accepted. A batch can contain up to **100 titles**. Each item is processed independently; failures do not stop the remaining items.

## Fields

### Required
- `media_type`: `"movie"` or `"tv"`.
- `stream.parts`: array of Telegram link objects, e.g. `{"url":"https://t.me/c/1234567890/10"}`. Alternatively use `stream.url` for one link.
- `manual_metadata.title`: title to create.
- For TV, `season_number` is required. `episode_number` is required only in `"fixed"` episode mode.

### `stream`
- `parts`: ordered array of Telegram source entries.
- `url`: optional single Telegram link instead of `parts`.
- `quality`: optional label such as `"1080p"`; leave empty/omit to detect it.
- `name`: optional stream/file display name.

A continuous Telegram interval is one inclusive range URL:
`https://t.me/c/CHANNEL_ID/FIRST-https://t.me/c/CHANNEL_ID/LAST`

Both endpoints must be in the same channel and ascending. Non-media/deleted posts are skipped; downloadable files are indexed in ascending message order. A range means separate files, not split parts of one file.

### `manual_metadata`
- `title` (string, required)
- `year` (number): release year.
- `rate` (number): optional rating, 0–10.
- `genres` (array or comma-separated string): e.g. `["FMGE"]`.
- `description` (string): overview.
- `poster` (string): poster image URL (the “post URL”).
- `backdrop` (string): optional backdrop image URL.

Omitted optional metadata may be filled with placeholders by the application.

### TV-only fields
- `episode_detection`: `"order"`, `"filename"`, or `"fixed"`.
- `season_number` (integer): season, including 0 if applicable.
- `episode_number` (integer): required only for `"fixed"`.
- `episode_title`, `episode_overview`, `episode_backdrop`, `episode_released`: optional episode metadata.

Episode modes:
- **`order`**: assign episode numbers in the order of Telegram files/messages. This is recommended when the channel posts episodes sequentially. The order mode appends after episodes already indexed for that title and season.
- **`filename`**: detect episode numbers from captions/filenames.
- **`fixed`**: attach all supplied files to one episode; set `episode_number`.

For the supplied `Prep: ENT RR` range 124–142, choose `episode_detection: "order"` and `season_number: 11`. The first downloadable media post is assigned the first episode number according to order mode; if the title/season already contains episodes, it appends after them. Ensure the season is empty if the first file must be episode 1.

### Optional catalogue/existing-title fields
- `catalog_ids`: array of custom catalogue IDs.
- `selected_id`: IMDb/TMDB ID for a selected metadata result.
- `tmdb_id` and `db_index`: add streams to an existing indexed title.
For normal AI-generated manual entries, omit these optional fields.

## AI instruction prompt

> Convert my media details into JSON for the Telegram-Stremio manual media addition feature. Follow `MANUAL_MEDIA_ADDITION_JSON.md` exactly. Return valid JSON only, using a top-level `items` array when I provide multiple titles. Create one item per distinct movie or series. Use `media_type` `"movie"` or `"tv"`. Put Telegram links in `stream.parts` in the order they should be processed; represent a continuous channel interval as one inclusive range URL. For series, include `season_number` and choose `episode_detection: "order"` when episodes are posted sequentially, `"filename"` only when episode numbers are present in filenames/captions, or `"fixed"` when all files belong to one specified episode. Put title, release year, genres, description and poster image URL in `manual_metadata`. Do not invent IDs, links, episode titles, years, genres, descriptions, or image URLs. Preserve supplied details exactly and omit unknown optional fields. Validate the JSON syntax.

## Example: supplied Prep: ENT RR details

```json
{
  "media_type": "tv",
  "stream": {
    "parts": [
      {"url": "https://t.me/c/4486527497/124-https://t.me/c/4486527497/142"}
    ]
  },
  "manual_metadata": {
    "title": "Prep: ENT RR",
    "year": 2026,
    "genres": ["FMGE"],
    "description": "Prepladder RR XI",
    "poster": "https://drive.fowlers.site/0:/Misc/prep.jpeg"
  },
  "episode_detection": "order",
  "season_number": 11
}
```
