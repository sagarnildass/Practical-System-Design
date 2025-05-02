export interface User {
  user_id: string;
  username: string;
  email: string;
  profile_picture_url: string;
  created_at: string;
}

export interface Post {
  post_id: string;
  user_id: string;
  username?: string;
  profile_picture_url?: string;
  content: string;
  post_type: 'TEXT' | 'IMAGE' | 'VIDEO' | 'LINK' | 'SHARE';
  created_at: string;
  parent_post_id?: string;
  likes_count: number;
  comments_count: number;
  shares_count: number;
  liked_by_me?: boolean;
}

export interface Comment {
  comment_id: string;
  post_id: string;
  user_id: string;
  username?: string;
  profile_picture_url?: string;
  content: string;
  created_at: string;
}

export interface Feed {
  posts: Post[];
}

export interface Followers {
  followers: User[];
}

export interface Following {
  following: User[];
}

export interface UserStats {
  posts_count: number;
  followers_count: number;
  following_count: number;
} 