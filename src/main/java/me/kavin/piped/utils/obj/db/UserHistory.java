package me.kavin.piped.utils.obj.db;

import jakarta.persistence.*;

@Entity
@Table(
    name = "user_history",
    uniqueConstraints = @UniqueConstraint(columnNames = {"user_id", "video_id"}, name = "user_history_user_video_uq"),
    indexes = {
        @Index(name = "user_history_user_id_idx", columnList = "user_id"),
        @Index(name = "user_history_watched_at_idx", columnList = "watched_at")
    }
)
public class UserHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private long userId;

    @Column(name = "video_id", length = 11, nullable = false)
    private String videoId;

    @Column(name = "title", length = 500)
    private String title;

    @Column(name = "uploader", length = 100)
    private String uploader;

    @Column(name = "uploader_url", length = 100)
    private String uploaderUrl;

    @Column(name = "thumbnail", length = 500)
    private String thumbnail;

    @Column(name = "duration")
    private int duration;

    @Column(name = "watched_at", nullable = false)
    private long watchedAt;

    @Column(name = "position_seconds")
    private double positionSeconds;

    public UserHistory() {}

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }
    public String getVideoId() { return videoId; }
    public void setVideoId(String videoId) { this.videoId = videoId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getUploader() { return uploader; }
    public void setUploader(String uploader) { this.uploader = uploader; }
    public String getUploaderUrl() { return uploaderUrl; }
    public void setUploaderUrl(String uploaderUrl) { this.uploaderUrl = uploaderUrl; }
    public String getThumbnail() { return thumbnail; }
    public void setThumbnail(String thumbnail) { this.thumbnail = thumbnail; }
    public int getDuration() { return duration; }
    public void setDuration(int duration) { this.duration = duration; }
    public long getWatchedAt() { return watchedAt; }
    public void setWatchedAt(long watchedAt) { this.watchedAt = watchedAt; }
    public double getPositionSeconds() { return positionSeconds; }
    public void setPositionSeconds(double positionSeconds) { this.positionSeconds = positionSeconds; }
}
